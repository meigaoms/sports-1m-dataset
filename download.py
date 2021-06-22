import argparse, json, os
import lib.config as config
import lib.parallel_download as parallel
from lib.cloud_storage import CloudStorage

def download_set(num_workers, failed_log, compress, verbose, skip, log_file, usage="train"):
    """
    Download the test set.
    :param num_workers:           Number of downloads in parallel.
    :param failed_log:            Where to save failed video ids.
    :param compress:              Decides if the videos should be compressed.
    :param verbose:               Print status.
    :param skip:                  Skip classes that already have folders (i.e. at least one video was downloaded).
    :param log_file:              Path to log file for youtube-dl.
    :return:
    """

    video_list = []
    if usage=="train":
        with open(config.TRAIN_METADATA_PATH, 'r') as fread:
            for line in fread.readlines():
                print(line)
                video_id, _ = line.split(" ")
                video_list.append(video_id[32:])
                break
        print(video_list)

    blob_video = CloudStorage(config.STORAGE_ACCOUNT_NAME, "sports-1m", config.CONNECTION_STRING, config.SAS_TOKEN)
    video_stored = set([os.path.basename(file_name) for file_name in blob_video.list_blob_names()])

    data_to_process = [each for each in video_list if each not in video_stored]


    pool = parallel.Pool(None, data_to_process, config.OUTPUT_ROOT, num_workers, failed_log, compress, verbose, skip,
                        log_file=log_file)
    pool.start_workers()
    pool.feed_videos()
    pool.stop_workers()

if __name__ == "__main__":
    parser = argparse.ArgumentParser("Download Kinetics videos in the mp4 format.")

    parser.add_argument("--categories", nargs="+", help="categories to download")
    parser.add_argument("--classes", nargs="+", help="classes to download")
    parser.add_argument("--all", action="store_true", help="download the whole dataset")
    parser.add_argument("--test", action="store_true", help="download the test set")

    parser.add_argument("--num-workers", type=int, default=1, help="number of downloader processes")
    parser.add_argument("--failed-log", default="dataset/failed.txt", help="where to save list of failed videos")
    parser.add_argument("--compress", default=False, action="store_true", help="compress videos using gzip (not recommended)")
    parser.add_argument("-v", "--verbose", default=False, action="store_true", help="print additional info")
    parser.add_argument("-s", "--skip", default=False, action="store_true", help="skip classes that already have folders")
    parser.add_argument("-l", "--log-file", help="log file for youtube-dl (the library used to download YouTube videos)")

    download_set(1, "OUTPUT/failed.txt", False, False, False, None)