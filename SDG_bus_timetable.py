# core
import os
import zipfile

# third party
import yaml
import pandas as pd

# our modules
import data_ingest as di

# get current working directory
CWD = os.getcwd()

# Load config
with open(os.path.join(CWD, "config.yaml")) as yamlfile:
    config = yaml.load(yamlfile, Loader=yaml.FullLoader)
    module = os.path.basename(__file__)
    print(f"Config loaded in {module}")

# Parameters
bus_timetable_zip_link = config["ENG_bus_timetable_data"]
bus_dataset_name = 'itm_all_gtfs'
output_directory = os.path.join(CWD, 'data', 'england_bus_timetable')
zip_path = os.path.join(output_directory, bus_dataset_name)
required_files = ['stop_times', 'trips', 'calendar']


# Create output directory
if not os.path.exists(output_directory):
    os.makedirs(output_directory)

# Download bus timetable data
# Using individual data ingest functions (rather than import_extract_delete_zip)
# as files are .txt not .csv.
di._grab_zip(file_nm=bus_dataset_name,
             zip_link=bus_timetable_zip_link,
             zip_path=zip_path)

# Extract the required files
for file in required_files:
    file_extension_name = f"{file}.txt"

    di._extract_zip(file_nm=bus_dataset_name,
                    csv_nm=file_extension_name,
                    zip_path=zip_path,
                    csv_path=output_directory)

# Remove zip file
di._delete_junk(file_nm=bus_dataset_name,
                zip_path=zip_path)

# Load the text files into pandas dataframes
# Specifying dtypes to remove unwanted columns and allow loading due to
# mixed data types in certain columns.
# Cannot parse datetime dtypes, and current function doesnt allow the parse_dates
# parameter in read_csv. So reading in as object type for the time being.
# Dont think they need to be datetime format as we are using every trip
# regardless of the date, and just want to extract the HH from the date which
# can be done from a string.

# stop times
stop_times_types = {'trip_id': 'category',
                    'departure_time': 'object', 'stop_id': 'category'}

stop_times_df = di._csv_to_df(file_nm='stop_times',
                              csv_path=os.path.join(
                                  output_directory, 'stop_times.txt'),
                              dtypes=stop_times_types)

# trips
trips_types = {'route_id': 'category',
               'service_id': 'category', 'trip_id': 'category'}

trips_df = di._csv_to_df(file_nm='trips',
                         csv_path=os.path.join(output_directory, 'trips.txt'),
                         dtypes=trips_types)

# calendar
calendar_types = {'service_id': 'category', 'monday': 'int64', 'tuesday': 'int64',
                  'wednesday': 'int64', 'thursday': 'int64', 'friday': 'int64',
                  'start_date': 'object', 'end_date': 'object'}

calendar_df = di._csv_to_df(file_nm='calendar',
                            csv_path=os.path.join(
                                output_directory, 'calendar.txt'),
                            dtypes=calendar_types)
