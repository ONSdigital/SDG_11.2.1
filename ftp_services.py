# Core imports for this module
from ftplib import FTP
from math import comb
import os

# FTP Variables
ftp_host = '162.241.253.72'
ftp_user = 'ons@ainslierockwell.com'
pass_path = 'ftp_pass.txt'
ftp = FTP(ftp_host)

# File Variables - move to file where function is called

#local_data_dir = "data/"
#remote_data_dir = "data/"

def retrieve_pass(pass_path):
    if os.path.isfile(pass_path):
        text_file = open(pass_path, 'r')
        pass_text = text_file.read()
        text_file.close()

        return(pass_text)
    
    else:
        raise Exception("check login details are being passed correctly")

def ftp_connect():
    ftp_pass = retrieve_pass(pass_path)
    print('connecting to...')
    print(ftp_host+'\n')
    print('attempting to connect as...')
    print(ftp_user+'\n')
    ftp.login(user=ftp_user, passwd=ftp_pass)
    print(ftp.getwelcome())
    #ftp.dir()
    #print('you are in directory: ' + ftp.pwd())

def ftp_get_file(file_to_get):
    print(f"Transferring: {file_to_get} ")
    with open(file_to_get, 'wb') as fp:
        ftp.retrbinary('RETR ' + file_to_get, fp.write)

def dict_iter(input_dict):
    ftp_connect()
    for remote_path, file_list in input_dict.items():
        ftp_get_directory(remote_path, file_list)
        print(f"Completed transfer of files from remote directory {remote_path}\n" )
    ftp.quit()

def ftp_get_directory(remote_data_dir, file_list):
    print(f"\nTransfer will deposit files to: {os.getcwd()}/{remote_data_dir}")
    home_dir = os.getcwd()
    ftp_home = ftp.pwd()
    if not os.path.isdir(remote_data_dir): 
        os.mkdir(remote_data_dir)
    os.chdir(remote_data_dir)
    ftp.cwd(remote_data_dir)
    for f in file_list:
        ftp_get_file(f)
    ftp.cwd(ftp_home)
    os.chdir(home_dir)
    

def ftp_send_file(file_to_send):
    ftp_connect()
    print('\n')
    print('attempting to send file...')
    print(file_to_send)
    print('\n')
    fileObject = open(file_to_send, 'rb')
    file2BeSavedAs = (file_to_send)
    ftp_Command = 'STOR %s'%file2BeSavedAs;
    ftp.storbinary(ftp_Command, fp=fileObject)
    ftp.dir()
    ftp.quit()

def ftp_listdir():
    ftp_connect()
    ftp.dir()
    print('you are in directory: ' + ftp.pwd())
    ftp.quit()

def list_datasets(directories):
    ftp.dir(directories)

def quit_ftp():
    ftp.quit()

def get_missing_files(remote_data_dir, file_list):
    home_dir = os.getcwd()
    ftp_home = ftp.pwd()
    ftp.cwd(remote_data_dir)

    if file_list is None:
        print("Proceeding with processing pipeline...")

    else:
        print("Initiating file transfer...") 
        for f in file_list:

            head_tail = os.path.split(f)
            dir = head_tail[0]
            file = head_tail[1]

            home_split = os.path.split(home_dir)
            home_split = home_split[0]

            dir_split = os.path.split(dir)

            if dir_split[0] == 'data': 
                combined_path = (f"{home_split}/{dir}")

                if os.path.split(dir_split[0])[0] == 'data':
                    sub_dir = os.path.split(dir_split[0])[1]
                    combined_path = (f"{home_split}/{sub_dir}/{dir_split[1]}")

            elif dir_split[0] != 'data':
                combined_path = (f"{home_dir}/{dir}")

            if not os.path.isdir(combined_path):
                    os.makedirs(combined_path)
                    os.chdir(combined_path)
                    ftp.cwd(ftp_home + dir)
                    ftp_get_file(file)
                    #os.chdir(os.path.pardir)
            
            elif os.path.isdir(combined_path):
                    os.chdir(combined_path)
                    ftp.cwd(ftp_home + dir)
                    ftp_get_file(file)
        
        print("All missing files transfered...")
        print("Proceeding with processing pipeline...")
