#logic to download all of the files from data directory on the remote server

from html import entities
from re import sub

from numpy import append, short
import ftp_services as ftp_services
import os, os.path

#keys = list(ftp_services.dir_list)

def create_recursive_data_directory_list():

    file_list = []
    data_dir = "data/"

    ftp_services.ftp_connect()
    entries = list(ftp_services.ftp.mlsd(data_dir))
    entries = [entry for entry in entries if entry[1]["type"] == "dir"]
    entries.sort(key = lambda entry: entry[1]['modify'], reverse = True)

    print(entries)

    append_list = []

    def sub_get(entries, data_dir):
        
        for e in entries:
            dir_list = data_dir + e[0]
            sub_entries = list(ftp_services.ftp.mlsd(dir_list))
            sub_entries = [entry for entry in sub_entries if entry[1]["type"] == "dir"]
            sub_entries.sort(key = lambda entry: entry[1]['modify'], reverse = True)
            append_list.append(dir_list)

            if (len(sub_entries)) >= 1:
                new_ddir = data_dir + e[0] + '/'
                sub_get(sub_entries, new_ddir)
    
    sub_get(entries, data_dir)
    append_list.append(data_dir)
    
    print(append_list)
    return append_list
    

def create_data_file_list(data_dirs):

    file_list = []
    #data_dir = "data/"

    for d in data_dirs:

        files = (list(ftp_services.ftp.mlsd(d)))
        files = [entry for entry in files if entry[1]["type"] == "file"]
        files.sort(key = lambda entry: entry[1]['modify'], reverse = True)
        
        for fl in range(len(files)):
            if d[-1] == '/':
                file_list.append(f"{d}{files[fl][0]}")
            
            elif d[-1] != '/':
                file_list.append(f"{d}/{files[fl][0]}")


    return file_list
"""
def create_local_file_list():

    data_dir = "data/"
    local_list = []
    local_dirs = []

    os.chdir(data_dir)

    for dir in list(filter(os.path.isdir, os.listdir())):
        local_dirs.append(dir)
        sub_dir_contents = os.scandir(dir)
    
        for sdc in sub_dir_contents:
            if os.path.isdir(sdc):
                #print(sdc.path)
                local_dirs.append(sdc.path)

    for file in list(filter(os.path.isfile, os.listdir())):
        local_list.append(f"{data_dir}{file}")

    for d in local_dirs:
        
        if d == data_dir[:-1]:
            print(f"setting root data dir")

        else:
            print(os.getcwd())            
            os.chdir(d)
            files = list(filter(os.path.isfile, os.listdir()))
            
            for f in files:
                local_list.append(f"{data_dir}{d}/{f}")
            os.chdir(os.path.pardir)

    return local_list"""

def create_local_file_list():
    path = f"{os.getcwd()}/data"
    sys_dir = f"{os.getcwd()}"
    #we shall store all the file names in this list
    #print(path)
    filelist = []
    
    for root, dirs, files in os.walk(path):
    
        for file in files:
            #append the file name to the list
            root = root.replace((sys_dir+'/'),"")
            print(root)
            filelist.append(os.path.join(root,file))
    
    print(filelist)
    return filelist


def check_missing_files(search_dirs):

    data_files = create_data_file_list(search_dirs)
    locl_files = create_local_file_list()

    set_remot = set(data_files)
    set_local = set(locl_files)

    missing_files = set_remot.difference(set_local)

    if len(missing_files) == 0:
        print("You are not missing any files")

    elif len(missing_files) >=1:
        print("You are missing the following required data files:")
        for f in missing_files:
            print(f)
        return list(missing_files)

def execute_file_grab(CWD):

    search_dirs = create_recursive_data_directory_list()
    missing_files = check_missing_files(search_dirs)
    data_dir = "data/"
    ftp_services.get_missing_files(data_dir, missing_files)
    os.chdir(CWD)
    