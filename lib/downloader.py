import subprocess
import os
from lib.cloud_storage import CloudStorage
import lib.config as config
import time


def download_video(video_id, download_path, video_format="mp4", log_file=None):
    """
    Download video from YouTube.
    :param video_id:        YouTube ID of the video.
    :param download_path:   Where to save the video.
    :param video_format:    Format to download.
    :param log_file:        Path to a log file for youtube-dl.
    :return:                Tuple: path to the downloaded video and a bool indicating success.
    """

    if log_file is None:
        stderr = subprocess.DEVNULL
    else:
        stderr = open(log_file, "a")

    return_code = subprocess.call(
        ["youtube-dl", "https://youtube.com/watch?v={}".format(video_id), "--quiet", "-f",
        "bestvideo[ext={}]+bestaudio/best".format(video_format), "--output", download_path, "--no-continue"], stderr=stderr)
    success = return_code == 0
    if log_file is not None:
        stderr.close()

    return success


def cut_video(raw_video_path, slice_path, start, end):
    """
    Cut out the section of interest from a video.
    :param raw_video_path:    Path to the whole video.
    :param slice_path:        Where to save the slice.
    :param start:             Start of the section.
    :param end:               End of the section.
    :return:                  Tuple: Path to the video slice and a bool indicating success.
    """

    return_code = subprocess.call(["ffmpeg", "-loglevel", "quiet", "-i", raw_video_path, "-strict", "-2",
                                    "-ss", str(start), "-to", str(end), slice_path])
    success = return_code == 0

    return success

  

def upload2blob(video_file):
    blob_video = CloudStorage(config.STORAGE_ACCOUNT_NAME, "sports-1m", config.CONNECTION_STRING, config.SAS_TOKEN)
    try:
        blob_video.upload_file(video_file, "/".join(video_file.split("/")[-3:]))
        time.sleep(1)
        os.remove(video_file)
    except Exception as e:
        print(f"Failed to remove {video_file}: {str(e)}")
        pass


def process_video(video_id, directory, start=None, end=None, video_format="mp4", compress=False, overwrite=False, log_file=None):
    """
    Process one video for the kinetics dataset.
    :param video_id:        YouTube ID of the video.
    :param directory:       Directory where to save the video.
    :param start:           Start of the section of interest.
    :param end:             End of the section of interest.
    :param video_format:    Format of the processed video.
    :param compress:        Decides if the video slice should be compressed by gzip.
    :param overwrite:       Overwrite processed videos.
    :param log_file:        Path to a log file for youtube-dl.
    :return:                Bool indicating success.
    """

    download_path = "{}.{}".format(os.path.join(directory, video_id), video_format)
    mkv_download_path = "{}.mkv".format(os.path.join(directory, video_id))
    slice_path = "{}.{}".format(os.path.join(directory, video_id), video_format)

    # simply delete residual downloaded videos
    if os.path.isfile(download_path):
        os.remove(download_path)

    # if sliced video already exists, decide what to do next
    img_files = [f_name for f_name in os.listdir(os.path.dirname(slice_path)) if f_name.endswith(".jpg") and f_name.startswith(video_id)]
    if img_files:
        return True
    if os.path.isfile(slice_path):
        if overwrite:
            os.remove(slice_path)
        else:
            return True

    # sometimes videos are downloaded as mkv
    if not os.path.isfile(mkv_download_path):
        # download video and cut out the section of interest
        success = download_video(video_id, download_path, log_file=log_file)

        if not success:
            return False

    # video was downloaded as mkv instead of mp4
    if not os.path.isfile(download_path) and os.path.isfile(mkv_download_path):
        download_path = mkv_download_path
        mp4file = mkv_download_path.replace("mkv", "mp4")
        convert_mkv2mp4 = ["ffmpeg", "-y", "-i", mkv_download_path, "-map", "0", "-c", "copy", "-c:a", "aac", mp4file, "-strict", "-2", "-loglevel", "fatal"]
        subprocess.run(convert_mkv2mp4)
        download_path = mp4file
        os.remove(mkv_download_path)
    
    upload2blob(download_path)

    if start and end:
        success = cut_video(download_path, slice_path, start, end)

        if not success:
            return False

    # remove the downloaded video
    # os.remove(mp4file)


    if compress:
        # compress the video slice
        pass

    return True


def video_worker(videos_queue, failed_queue, compress, log_file):
    """
    Downloads videos pass in the videos queue.
    :param videos_queue:      Queue for metadata of videos to be download.
    :param failed_queue:      Queue of failed video ids.
    :param compress:          Whether to compress the videos using gzip.
    :param log_file:          Path to a log file for youtube-dl.
    :return:                  None.
    """

    while True:
        request = videos_queue.get()

        if request is None:
            break

        video_id, directory, start, end = request

        if not process_video(video_id, directory, start, end, compress=compress, log_file=log_file):
            failed_queue.put(video_id)


class Pool:
  """
  A pool of video downloaders.
  """

  def __init__(self, classes, source_directory, target_directory, num_workers, failed_save_file):
    self.classes = classes
    self.source_directory = source_directory
    self.target_directory = target_directory
    self.num_workers = num_workers
    self.failed_save_file = failed_save_file

    self.videos_queue = Queue(100)
    self.failed_queue = Queue(100)

    self.workers = []
    self.failed_save_worker = None

  def feed_videos(self):
    """
    Feed videos to a queue for workers.
    :return:      None.
    """

    if self.classes is None:
      videos = os.listdir(self.source_directory)

      for filename in videos:
        video_path = os.path.join(self.source_directory, filename)
        video_id = ".".join(filename.split(".")[:-1])
        target_dir_path = os.path.join(self.target_directory, video_id)
        self.videos_queue.put((video_id, video_path, target_dir_path))
    else:
      for class_name in self.classes:
        source_class_dir = os.path.join(self.source_directory, class_name.replace(" ", "_"))
        target_class_dir = os.path.join(self.target_directory, class_name.replace(" ", "_"))

        if os.path.isdir(source_class_dir):

          if not os.path.isdir(target_class_dir):
            # when using multiple processes, the folder might have been already created (after the if was evaluated)
            try:
              os.makedirs(target_class_dir)
            except FileExistsError:
              pass

          videos = os.listdir(source_class_dir)

          for filename in videos:
            video_path = os.path.join(source_class_dir, filename)
            video_id = ".".join(filename.split(".")[:-1])
            target_dir_path = os.path.join(target_class_dir, video_id)
            self.videos_queue.put((video_id, video_path, target_dir_path))

  def start_workers(self):
    """
    Start all workers.
    :return:    None.
    """

    # start failed videos saver
    if self.failed_save_file is not None:
      self.failed_save_worker = Process(target=write_failed_worker, args=(self.failed_queue, self.failed_save_file))
      self.failed_save_worker.start()

    # start download workers
    for _ in range(self.num_workers):
      worker = Process(target=video_worker, args=(self.videos_queue, self.failed_queue))
      worker.start()
      self.workers.append(worker)

  def stop_workers(self):
    """
    Stop all workers.
    :return:    None.
    """

    # send end signal to all download workers
    for _ in range(len(self.workers)):
      self.videos_queue.put(None)

    # wait for the processes to finish
    for worker in self.workers:
      worker.join()

    # end failed videos saver
    if self.failed_save_worker is not None:
      self.failed_queue.put(None)
      self.failed_save_worker.join()

def download_class_parallel(class_name, videos_list, directory, videos_queue):
  """
  Download all videos of the given class in parallel.
  :param class_name:        Name of the class.
  :param videos_list:       List of all videos.
  :param directory:         Where to save the videos.
  :param videos_queue:      Videos queue for parallel download.
  :return:                  None.
  """

  if class_name is None:
    class_dir = directory
  else:
    class_dir = os.path.join(directory, class_name.replace(" ", "_"))

  if not os.path.isdir(class_dir):
    # when using multiple processes, the folder might have been already created (after the if was evaluated)
    try:
      os.mkdir(class_dir)
    except FileExistsError:
      pass

  for video in videos_list:

      videos_queue.put((video, class_dir, None, None))