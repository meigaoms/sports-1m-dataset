import os
import os.path as op
import logging
# from ete3 import Tree
from azure.storage.blob import BlobClient, BlobServiceClient
import subprocess as sp


def ensure_directory(path):
    """Check exsitence of the given directory path. If not, create a new directory.

    Args:
        path (str): path of a given directory.
    """
    if path == '' or path == '.':
        return
    if path != None and len(path) > 0:
        assert not op.isfile(path), '{} is a file'.format(path)
        if not os.path.exists(path) and not op.islink(path):
            try:
                os.makedirs(path)
            except:
                if os.path.isdir(path):
                    # another process has done makedir
                    pass
                else:
                    raise
        # we should always check if it succeeds.
        assert op.isdir(op.abspath(path)), path

def cmd_run(list_cmd, return_output=False, env=None,
        working_dir=None,
        stdin=sp.PIPE,
        shell=False,
        dry_run=False,
        ):
    logging.info('start to cmd run: {}'.format(' '.join(map(str, list_cmd))))
    # if we dont' set stdin as sp.PIPE, it will complain the stdin is not a tty
    # device. Maybe, the reson is it is inside another process.
    # if stdout=sp.PIPE, it will not print the result in the screen
    e = os.environ.copy()
    if 'SSH_AUTH_SOCK' in e:
        del e['SSH_AUTH_SOCK']
    if working_dir:
        ensure_directory(working_dir)
    if env:
        for k in env:
            e[k] = env[k]
    if dry_run:
        # we need the log result. Thus, we do not return at teh very beginning
        return
    if not return_output:
        #if env is None:
            #p = sp.Popen(list_cmd, stdin=sp.PIPE, cwd=working_dir)
        #else:
        if shell:
            p = sp.Popen(' '.join(list_cmd),
                    stdin=stdin,
                    env=e,
                    cwd=working_dir,
                    shell=True)
        else:
            p = sp.Popen(list_cmd,
                    stdin=sp.PIPE,
                    env=e,
                    cwd=working_dir)
        message = p.communicate()
        if p.returncode != 0:
            raise ValueError(message)
    else:
        if shell:
            message = sp.check_output(' '.join(list_cmd),
                    env=e,
                    cwd=working_dir,
                    shell=True)
        else:
            message = sp.check_output(list_cmd,
                    env=e,
                    cwd=working_dir)
        logging.info('finished the cmd run')
        return message.decode('utf-8')

# def get_leaf_names(all_fname):
#     # build the tree first
#     root = Tree()
#     for fname in all_fname:
#         components = fname.split('/')
#         curr = root
#         for com in components:
#             currs = [c for c in curr.children if c.name == com]
#             if len(currs) == 0:
#                 curr = curr.add_child(name=com)
#             else:
#                 assert len(currs) == 1
#                 curr = currs[0]
#     result = []
#     for node in root.iter_leaves():
#         ans = [s.name for s in node.get_ancestors()[:-1]]
#         ans.insert(0, node.name)
#         result.append('/'.join([a for a in ans[::-1]]))
#     return result



class CloudStorage(object):
    def __init__(self, account_name, container_name, connection_string, sas_token):
        self.account_name = account_name
        self.container_name = container_name
        # blob_service_client is for azure.storage.blob service
        self.blob_service_client = BlobServiceClient.from_connection_string(connection_string)
        # sas_token and account_name are used in azcopy
        self.sas_token = sas_token
        

    def list_blob_names(self, name_starts_with=None):
        container_client = self.blob_service_client.get_container_client(self.container_name)
        blob_list = container_client.list_blobs(name_starts_with=name_starts_with)
        return [blob.name for blob in blob_list]

    def upload_folder(self, folder, target_prefix):
        def remove_tailing(x):
            if x.endswith('/') or x.endswith('\\'):
                x = x[:-1]
            return x
        folder = remove_tailing(folder)
        target_prefix = remove_tailing(target_prefix)
        for root, dirs, files in os.walk(folder):
            for f in files:
                src_file = op.join(root, f)
                assert src_file.startswith(folder)
                target_file = src_file.replace(folder, target_prefix)
                self.upload_file(src_file, target_file)
            for d in dirs:
                self.upload_folder(op.join(root, d),
                        op.join(target_prefix, d))

    def upload_file(self, src_file, target_file):
        logging.info('uploading {} to {}'.format(src_file, target_file))
        if target_file.startswith('/'):
            logging.info('remove strarting slash for {}'.format(target_file))
            target_file = target_file[1:]
        blob_client = self.blob_service_client.get_blob_client(self.container_name, target_file)
        with open(src_file, "rb") as data:
            blob_client.upload_blob(data)

    def az_sync(self, src_dir, dest_dir):
        assert self.sas_token
        cmd = []
        cmd.append(get_azcopy())
        cmd.append('sync')
        cmd.append(op.realpath(src_dir))
        url = 'https://{}.blob.core.windows.net'.format(self.account_name)
        if dest_dir.startswith('/'):
            dest_dir = dest_dir[1:]
        url = op.join(url, self.container_name, dest_dir)
        assert self.sas_token.startswith('?')
        data_url = url
        url = url + self.sas_token
        cmd.append(url)
        if op.isdir(src_dir):
            cmd.append('--recursive')
        cmd_run(cmd)
        return data_url, url

    def az_upload(self, src_dir, dest_dir, sync=False):
        assert self.sas_token
        cmd = []
        cmd.append(get_azcopy())
        if sync:
            cmd.append('sync')
        else:
            cmd.append('cp')
        cmd.append(op.realpath(src_dir))
        url = 'https://{}.blob.core.windows.net'.format(self.account_name)
        if dest_dir.startswith('/'):
            dest_dir = dest_dir[1:]
        url = op.join(url, self.container_name, dest_dir)
        assert self.sas_token.startswith('?')
        data_url = url
        url = url + self.sas_token
        cmd.append(url)
        if op.isdir(src_dir):
            cmd.append('--recursive')
        cmd_run(cmd)
        return data_url, url

    def az_download_all(self, local_dir):
        all_blob_name = list(self.list_blob_names())
        all_blob_name = get_leaf_names(all_blob_name)
        for blob_name in all_blob_name:
            target_file = os.path.join(local_dir, blob_name)
            if not op.isfile(target_file):
                self.az_download(blob_name, target_file,
                        sync=False)

    def az_download(self, remote_path, local_path, sync=True, is_folder=False):
        ensure_directory(op.dirname(local_path))
        origin_local_path = local_path
        local_path = local_path + '.tmp'
        ensure_directory(op.dirname(local_path))
        assert self.sas_token
        cmd = []
        cmd.append(op.expanduser(config.AZCOPY_PATH))
        if sync:
            cmd.append('sync')
        else:
            cmd.append('copy')
        url = 'https://{}.blob.core.windows.net'.format(self.account_name)
        if remote_path.startswith('/'):
            remote_path = remote_path[1:]
        url = '/'.join([url, self.container_name, remote_path])
        assert self.sas_token.startswith('?')
        data_url = url
        url = url + self.sas_token
        cmd.append(url)
        cmd.append(op.realpath(local_path))
        if is_folder:
            cmd.append('--recursive')
            if sync:
                # azcopy's requirement
                ensure_directory(local_path)
        cmd_run(cmd)
        os.rename(local_path, origin_local_path)
        return data_url, url

    # def download_to_path(self, blob_name, local_path):
    #     dir_path = op.dirname(local_path)
        
    #     if op.isfile(dir_path) and get_file_size(dir_path) == 0:
    #         os.remove(dir_path)
    #     ensure_directory(dir_path)
    #     blob_client = self.blob_service_client.get_blob_client(container=self.container_name, blob=blob_name)
    #     with open(local_path, "wb") as download_file:
    #         download_file.write(blob_client.download_blob().readall())



if __name__ == '__main__':
    # Please specify AZCOPY_PATH in onedata/defaults.py if necessary
    storage_account_name = ""
    container_name = ""
    connection_string = ""
    sas_token = ""
    my_blob = CloudStorage(storage_account_name, container_name, connection_string, sas_token)
    blob_list = my_blob.list_blob_names()
    my_blob.list_blob_names()