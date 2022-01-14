#logic to download all of the files from data directory on the remote server

from re import sub
import ftp_services as ftp_services
import os, os.path

keys = list(ftp_services.dir_list)

def grab_files():

    ftp_services.ftp_connect()

    for k in keys:
        print (k)
        for name in (ftp_services.ftp.nlst(k)):
            print(name)

    ftp_services.quit_ftp()


def create_recursive_data_file_list():

    file_list = []
    data_dir = "test_directory/"

    ftp_services.ftp_connect()
    entries = list(ftp_services.ftp.mlsd(data_dir))
    entries = [entry for entry in entries if entry[1]["type"] == "dir"]
    entries.sort(key = lambda entry: entry[1]['modify'], reverse = True)

    def sub_get(entries, data_dir):
        for e in entries:
            dir_list = data_dir + e[0]
            sub_entries = list(ftp_services.ftp.mlsd(dir_list))
            sub_entries = [entry for entry in sub_entries if entry[1]["type"] == "dir"]
            sub_entries.sort(key = lambda entry: entry[1]['modify'], reverse = True)

            print(e[0])
            print (dir_list)
            print (len(sub_entries))

            if (len(sub_entries)) >= 1:
                new_ddir = data_dir + e[0] + '/'
                sub_get(sub_entries, new_ddir)
    
    sub_get(entries, data_dir)
    
    """
    def recursive_list(remotepath):
        sub_directories = []
        for entry in entries:
            if entry[1]['type'] == 'dir':
                remotepath = data_dir + entry[0]
                sub_directories.append(remotepath)
            else:
                print(entry)
        return sub_directories

    sub_directories = recursive_list('data/')
    print(f"subdir length: {len(sub_directories)}")
    print(sub_directories)
    
    def sub_sub_list(sub_directories):
        for d in sub_directories:
            sub_directories = []
            print(f"sub-directory: {d}")
            entries = list(ftp_services.ftp.mlsd(d))
            entries = [entry for entry in entries if entry[1]["type"] == "dir"]
            entries.sort(key = lambda entry: entry[1]['modify'], reverse = True)
            for entry in entries:
                if entry[1]['type'] == 'dir':
                    remotepath = d + '/' + entry[0]
                    print(remotepath)
                    sub_directories.append(remotepath)
                else:
                    print(entry)
            
            return sub_directories

    sub_sub_directories = sub_sub_list(sub_directories)

    print(len(sub_directories))
    print(sub_directories)
    print(len(sub_sub_directories))
    print(sub_sub_directories)

    if len(sub_sub_directories) >= 1:
        print(sub_sub_list(sub_sub_directories))
    
    #print(list(ftp_services.ftp.mlsd(sub_directories[0])))
    """

def create_data_file_list():

    file_list = []
    data_dir = "data/"

    ftp_services.ftp_connect()
    entries = list(ftp_services.ftp.mlsd(data_dir))
    entries = [entry for entry in entries if entry[1]["type"] == "dir"]
    entries.sort(key = lambda entry: entry[1]['modify'], reverse = True)
    print(entries)

    files = (list(ftp_services.ftp.mlsd(data_dir)))
    files = [entry for entry in files if entry[1]["type"] == "file"]
    files.sort(key = lambda entry: entry[1]['modify'], reverse = True)

    for fl in range(len(files)):
        file_list.append(f"{files[fl][0]}")

    for num in range(len(entries)):

        files = list(ftp_services.ftp.mlsd(data_dir + str(entries[num][0])))
        files = [entry for entry in files if entry[1]["type"] == "file"]
        files.sort(key = lambda entry: entry[1]['modify'], reverse = True)

        for fl in range(len(files)):
            file_list.append(f"{entries[num][0]}/{files[fl][0]}")


    #print (file_list)

    return file_list

def create_local_file_list():

    data_dir = "data/"
    local_list = []
    local_dirs = []

    os.chdir(data_dir)

    for dir in list(filter(os.path.isdir, os.listdir())):
        local_dirs.append(dir)

    for file in list(filter(os.path.isfile, os.listdir())):
        local_list.append(file)

    for d in local_dirs:
        
        os.chdir(d)
        files = list(filter(os.path.isfile, os.listdir()))
        
        for f in files:
            local_list.append(f"{d}/{f}")
        os.chdir(os.path.pardir)

    #print(local_dirs)

    return local_list

def check_missing_files():

    data_files = create_data_file_list()
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

    create_recursive_data_file_list()
    #missing_files = check_missing_files()
    #data_dir = "data/"
    #ftp_services.get_missing_files(data_dir, missing_files)
    #os.chdir(CWD)
    