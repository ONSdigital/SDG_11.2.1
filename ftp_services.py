# Core imports for this module
from ftplib import FTP

ftp_host = '162.241.253.72'
ftp_user = 'ons@ainslierockwell.com'
ftp_pass = ftp_pass.txt

def ftp_connect():
    ftp = FTP(ftp_host)
    ftp.login(user=ftp_user, passwd=ftp_pass)
    print(ftp.getwelcome())
    print('connected to host: ' + ftp_host)
    print('logged in as user: ' + ftp_user)
    ftp.dir()
    print('you are in directory: ' + ftp.pwd())

ftp_connect()

#with open('readme.txt', 'wb') as fp:
#    ftp.retrbinary('RETR readme.txt', fp.write)
