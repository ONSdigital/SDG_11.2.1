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

local_data_dir = "data/"
remote_data_dir = "data/"

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
             "data/population_estimates/" : ["westmids_pop_only.csv"],
             "data/pop_weighted_centroids/" : ["*.*"]}

dir_list = ["data/", 
            "data/LSOA_shp/", 
            "data/population_estimates/", 
            "data/pop_weighted_centroids/"]

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
    #print(f"\nTransfer will deposit files to: {os.getcwd()}")
    home_dir = os.getcwd()
    ftp_home = ftp.pwd()
    print("about to retrieve missing files")

    print(home_dir)

    #print(file_list)

    ftp.cwd(remote_data_dir)

    if file_list is None:
        print("You are not missing any data files! Congratulationss")

    else: 
        for f in file_list:

            head_tail = os.path.split(f)
            dir = head_tail[0]
            file = head_tail[1]

            home_split = os.path.split(home_dir)
            home_split = home_split[0]
            
            combined_path = (f"{home_split}/{dir}")

            #print(f"ftp dir: {ftp.pwd()}")
            #print(f"remote: data dir: {remote_data_dir}")
            #print(f"ftp_home: {ftp_home}")

            if not os.path.isdir(combined_path):
                    os.makedirs(combined_path)
                    os.chdir(combined_path)
                    ftp.cwd(ftp_home + dir)
                    ftp_get_file(file)
                    #os.chdir(os.path.pardir)
            
            elif os.path.isdir(combined_path):
                    print("directory_exists")
                    os.chdir(combined_path)
                    ftp.cwd(ftp_home + dir)
                    ftp_get_file(file)

            """
            
            if "/" in f:
                list = f.split("/")
                dir = list[0] + "/"
                file = list[1]

                if not os.path.isdir(dir):
                    os.mkdir(dir)
                    os.chdir(dir)
                    ftp.cwd(dir)
                    ftp_get_file(file)
                    ftp.cwd(ftp_home + remote_data_dir)
                    os.chdir(os.path.pardir)
                else:
                    os.chdir(dir)
                    ftp.cwd(dir)
                    ftp_get_file(file)
                    ftp.cwd(ftp_home + remote_data_dir)
                    os.chdir(os.path.pardir)

            else:
                ftp_get_file(f)
            """
