#logic to download all of the files from data directory on the remote server

import ftp_services as ftp_services

#ftp_services.ftp_listdir()

#ftp_services.list_datasets("data")

#print(ftp_services.file_dict.keys())

#print(ftp_services.file_dict["data/"])

#ftp_services.list_datasets("data/pop_weighted_centroids/")

keys = list(ftp_services.dir_list)

print(keys)

def grab_files():

    ftp_services.ftp_connect()

    for k in keys:
        print (k)
        for name in (ftp_services.ftp.nlst(k)):
            print(name)

    ftp_services.quit_ftp()


#grab_files()

ftp_services.ftp_connect()
entries = list(ftp_services.ftp.mlsd("data/"))
entries = [entry for entry in entries if entry[1]["type"] == "dir"]
entries.sort(key = lambda entry: entry[1]['modify'], reverse = True)

print(len(entries))

for num in range(len(entries)):
    print(entries[num][0])    


files = list(ftp_services.ftp.mlsd("data/"))
files = [entry for entry in files if entry[1]["type"] == "file"]
files.sort(key = lambda entry: entry[1]['modify'], reverse = True)

print(len(files))

for num in range(len(files)):
    print(files[num][0])    