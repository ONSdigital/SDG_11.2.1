#core imports for this module
from html import entities
from re import sub
from numpy import append, short
import ftp_services as ftp_services
import os, os.path

def create_recursive_data_directory_list():
    """
    This function creates a list of all the sub directories that are contained
    on the remote ftp server within the root data directroy.

    Returns:
        append_list: list containing strings of all the remote directories.
    """
    data_dir = "data/"

    #the following four lines create a list of entries which are the remote directories
    ftp_services.ftp_connect()
    entries = list(ftp_services.ftp.mlsd(data_dir))
    entries = [entry for entry in entries if entry[1]["type"] == "dir"]
    entries.sort(key = lambda entry: entry[1]['modify'], reverse = True)
    
    append_list = []

    def sub_get(entries, data_dir):
        """
        Retrives directory listings for all subdirectoreis within the data_dir.

        Directories are appended to append_list as strings.

        Args:
            entries: list of remote sub directories.
            data_dir: string containing the root remote data direcory.
        """
        
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
    
    return append_list


def create_data_file_list(data_dirs):
    """
    Takes the passed list of remote data directories and generates a list
    of all files which are contained within these directories.

    Args:
        data_dirs: list containing strings of all the remote directories.

    Returns: 
        file_list: list containing strings of files with their full directory
        listing and extension.
    """

    file_list = []

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

def create_local_file_list():
    """
    Creates a list of the local data files

    Returns:
        filelist: list containing strings of the local data files
    """

    path = f"{os.getcwd()}/data"
    sys_dir = f"{os.getcwd()}"
    #we shall store all the file names in this list
    filelist = []
    
    for root, dirs, files in os.walk(path):
    
        for file in files:
            #append the file name to the list
            root = root.replace((sys_dir+'/'),"")
            filelist.append(os.path.join(root,file))
    
    return filelist

def check_missing_files(search_dirs):
    """
    Checks which files are missing by comparing set of the remote data files with
    set of the local data files.

    Any missing files are appended to the list of missing files.

    Args:
        search_dirs: list of the remote data directories to search to generate file list.

    Returns:
        missing_files: list containing strings of all the missing files, including their
        enclosing directroy structure and extension.
    """

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
    