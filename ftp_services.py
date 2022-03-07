# Core imports for this module
from ftplib import FTP
from math import comb
import os

# FTP Details Required for Login
ftp_host = '162.241.253.72'
ftp_user = 'ons@ainslierockwell.com'
pass_path = 'ftp_pass.txt'
ftp = FTP(ftp_host)

def retrieve_pass(pass_path):
    """
    Retrieves the text of ftp password from the text file that contains it.

    Args:
        pass_path: the full name of text file containing password.

    Returns: 
        pass_text: string containing the ftp login password.
    """

    if os.path.isfile(pass_path):
        text_file = open(pass_path, 'r')
        pass_text = text_file.read()
        text_file.close()

        return(pass_text)
    
    else:
        raise Exception("check login details are being passed correctly")

def ftp_connect():
    """
    Function that connects to the ftp server.
    """
    ftp_pass = retrieve_pass(pass_path)
    print('connecting to...')
    print(ftp_host+'\n')
    print('attempting to connect as...')
    print(ftp_user+'\n')
    ftp.login(user=ftp_user, passwd=ftp_pass)
    print(ftp.getwelcome())

def ftp_get_file(file_to_get):
    """
    Function which downloads the file that is passed into it.

    Args: 
        file_to_get: string containing the path and extension of the file
        that is to be downloaded.
    """
    print(f"Transferring: {file_to_get} ")
    with open(file_to_get, 'wb') as fp:
        ftp.retrbinary('RETR ' + file_to_get, fp.write)


def quit_ftp():
    """
    Quits and closes ftp connection
    """
    ftp.quit()

def get_missing_files(remote_data_dir, file_list):
    """
    Contains logic for downloading all of the files deemed to be missing from
    local directories.

    Takes the passed file list and remote data directory and downloads all files
    contained in the list to the relevant local directory. This maintains subdirectory
    structure on local drive.

    Args:
        remote_data_dir: string containing root remote data directory
        file_list: list containing strings which detail the subdirectory, filename
        and extension of the missing file to be downloaded.
    """

    home_dir = os.getcwd()
    ftp_home = ftp.pwd()
    ftp.cwd(remote_data_dir)

    if file_list is None:
        print("Proceeding with processing pipeline...")

    else:
        print("Initiating file transfer...") 
        for f in file_list:

            #splits the current file into its directory and file elements            
            head_tail = os.path.split(f)
            dir = head_tail[0]
            file = head_tail[1]
            
            #creates combined path from the passed file details and the home_dir
            combined_path = (f"{home_dir}/{dir}")

            #if the current file is in a directory that does not already exist the function will create it prior to downloading
            if not os.path.isdir(combined_path):
                    os.makedirs(combined_path)
                    os.chdir(combined_path)
                    ftp.cwd(ftp_home + dir)
                    ftp_get_file(file)
            
            #if the file is in a directory that does exist then the function will proceed straight to downloading
            elif os.path.isdir(combined_path):
                    os.chdir(combined_path)
                    ftp.cwd(ftp_home + dir)
                    ftp_get_file(file)
        
        print("All missing files transfered...")
        print("Proceeding with processing pipeline...")
