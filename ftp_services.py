# Core imports for this module
from ftplib import FTP
import os

# FTP Variables
ftp_host = '162.241.253.72'
ftp_user = 'ons@ainslierockwell.com'
pass_path = 'ftp_pass.txt'
ftp = FTP(ftp_host)

# File Variables - move to file where function is called

local_data_dir = "data/"
remote_data_dir = "data/"

#file_list = ["KS101EW-usual_resident_population.csv",
#             "Output_Areas__December_2011__Boundaries_EW_BGC.csv",
#             "RUC11_OA11_EW.csv",
#             "nomis_QS104EW.csv",
#             "nomis_QS303.csv"]
#
#             ["Lower_Layer_Super_Output_Areas__December_2011__Boundaries_EW_BGC.cpg",
#             "Lower_Layer_Super_Output_Areas__December_2011__Boundaries_EW_BGC.dbf",
#             "Lower_Layer_Super_Output_Areas__December_2011__Boundaries_EW_BGC.prj",
#             "Lower_Layer_Super_Output_Areas__December_2011__Boundaries_EW_BGC.shp",
#             "Lower_Layer_Super_Output_Areas__December_2011__Boundaries_EW_BGC.shx",
#             "Lower_Layer_Super_Output_Areas__December_2011__Boundaries_EW_BGC.xml"]
#             
#             ["population_estimates/westmids_pop_only.csv"]
#

file_dict = {"data/" : ["KS101EW-usual_resident_population.csv",
             "Output_Areas__December_2011__Boundaries_EW_BGC.csv",
             "RUC11_OA11_EW.csv",
             "nomis_QS104EW.csv",
             "nomis_QS303.csv"],
             "data/LSOA_shp/" : ["Lower_Layer_Super_Output_Areas__December_2011__Boundaries_EW_BGC.cpg",
             "Lower_Layer_Super_Output_Areas__December_2011__Boundaries_EW_BGC.dbf",
             "Lower_Layer_Super_Output_Areas__December_2011__Boundaries_EW_BGC.prj",
             "Lower_Layer_Super_Output_Areas__December_2011__Boundaries_EW_BGC.shp",
             "Lower_Layer_Super_Output_Areas__December_2011__Boundaries_EW_BGC.shx",
             "Lower_Layer_Super_Output_Areas__December_2011__Boundaries_EW_BGC.xml"],
             "data/population_estimates/" : ["westmids_pop_only.csv"]}

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
    #ftp_connect()
    print("attempting to retrive:")
    print(file_to_get)
    with open(file_to_get, 'wb') as fp:
        ftp.retrbinary('RETR ' + file_to_get, fp.write)
    #ftp.quit()

def dict_iter(input_dict):
    ftp_connect()
    for remote_path, file_list in file_dict.items():
        ftp_get_directory(remote_path, file_list)
        print(f"transfer of files in {remote_path} completed" )
    
    ftp.quit()

def ftp_get_directory(remote_data_dir, file_list):
    print("copying files to: " + os.getcwd())
    home_dir = os.getcwd()
    ftp_home = ftp.pwd()
    if not os.path.isdir(remote_data_dir): 
        os.mkdir(remote_data_dir)
    os.chdir(remote_data_dir)
    ftp.cwd(remote_data_dir)
    print(f"currently in remote dir: {ftp.pwd()}")
    print(ftp.dir())
    for f in file_list:
        ftp_get_file(f)
    ftp.cwd(ftp_home)
    os.chdir(home_dir)
    print(os.listdir())

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
    ftp_connect()
    ftp.dir(*directories)
    ftp.quit()

dict_iter(file_dict)

#ftp_get_directory(local_data_dir, remote_data_dir, file_list)
#ftp_getfile('readme.txt')
#ftp_sendfile('send_file_test.txt')
#ftp_listdir()