# Core imports for this module
from ftplib import FTP
import os

ftp_host = '162.241.253.72'
ftp_user = 'ons@ainslierockwell.com'
pass_path = 'ftp_pass.txt'
ftp = FTP(ftp_host)

def retrieve_pass(pass_path):
    if os.path.isfile(pass_path):
        text_file = open(pass_path, 'r')
        pass_path = text_file.read()
        text_file.close()

        return(pass_path)

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

def ftp_getfile(file_to_get):
    ftp_connect()
    with open(file_to_get, 'wb') as fp:
        ftp.retrbinary('RETR ' + file_to_get, fp.write)
    ftp.quit()

def ftp_sendfile(file_to_send):
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


#ftp_connect()
#ftp_getfile('readme.txt')
#ftp_sendfile('send_file_test.txt')
#ftp_listdir()