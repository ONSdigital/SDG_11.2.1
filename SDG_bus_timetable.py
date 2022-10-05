# core
import os

# third party
import yaml
import pandas as pd

# our modules
import data_ingest as di
import data_transform as dt

# get current working directory
CWD = os.getcwd()

# Load config
with open(os.path.join(CWD, "config.yaml")) as yamlfile:
    config = yaml.load(yamlfile, Loader=yaml.FullLoader)
    module = os.path.basename(__file__)
    print(f"Config loaded in {module}")

# Parameters
DATA_DIR = config[DATA_DIR]
bus_timetable_zip_link = config["ENG_bus_timetable_data"]
bus_dataset_name = 'itm_all_gtfs'
output_directory = os.path.join(DATA_DIR, 'england_bus_timetable')
zip_path = os.path.join(output_directory, bus_dataset_name)
required_files = ['stop_times', 'trips', 'calendar']
auto_download_bus = config["auto_download_bus"]


# Calculate if bus timetable needs to be downloaded.
# If current folder doesnt exist, or hasnt been modified for
# 7 days then redownload the zip

if not di._persistent_exists(output_directory):
    os.makedirs(output_directory)
    download_bus_timetable = True
else:
    # Find when the last download occured
    # If > 7 days ago, download the data again
    download_bus_timetable = di.best_before(path=output_directory,
                                            number_of_days=7)

# ---------------------------
# Download bus timetable data
# ---------------------------

# Using individual data ingest functions (rather than import_extract_delete_zip)
# as files are .txt not .csv.
if download_bus_timetable and auto_download_bus:
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

# ------------------------------------------
# Load the text files into pandas dataframes
# ------------------------------------------

# Specify dtypes to remove unwanted columns and allow loading due to
# mixed data types in certain columns.
# Cannot parse datetime dtypes, and current function doesnt allow the parse_dates
# parameter in read_csv. So reading in as object type for the time being.
# Dont think they need to be datetime format as we are using every trip
# regardless of the date, and just want to extract the HH from the date which
# can be done from a string.

# Stop times
feath_ = os.path.join(output_directory, "stop_times.feather")
if os.path.exists(feath_):
    stop_times_df = di._feath_to_df("stop_times", feath_)
else:
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
# NOTE: Not adding in saturday and sunday columns for stops because
# we are only interested in weekday trips for highly serviced stops
calendar_types = {'service_id': 'category', 'monday': 'int64', 'tuesday': 'int64',
                  'wednesday': 'int64', 'thursday': 'int64', 'friday': 'int64',
                  'start_date': 'object', 'end_date': 'object'}

calendar_df = di._csv_to_df(file_nm='calendar',
                            csv_path=os.path.join(
                                output_directory, 'calendar.txt'),
                            dtypes=calendar_types)

# ----------
# Clean data
# ----------

# Some departure times are > 24:00 so need to be removed
hour_range = range(config["early_bus_hour"],config["late_bus_hour"])
valid_hours = [f'0{i}' if i < 10 else f'{i}' for i in hour_range]

stop_times_df = stop_times_df[stop_times_df['departure_time'].str.startswith(tuple(valid_hours))]

# Convert start and end date to datetime format
calendar_df['start_date'] = pd.to_datetime(calendar_df['start_date'], format='%Y%m%d')
calendar_df['end_date'] = pd.to_datetime(calendar_df['end_date'], format='%Y%m%d')

# ----------------
# Join data frames
# ----------------

bus_timetable_df = (
    (stop_times_df.merge(trips_df, on='trip_id', how='left'))
    .merge(calendar_df, on='service_id', how='left')
)

# Remove columns no longer required
# NOTE: Route_id not used at all so could be removed at load
bus_timetable_df = bus_timetable_df.drop(columns=['trip_id', 'route_id']) # 'service_id'


# ----------------------------
# Extract stops for chosen day
# ----------------------------
single_year_bus_stops = dt.filter_by_year(bus_timetable_df)
serviced_bus_stops = dt.filter_bus_timetable_by_day(single_year_bus_stops, "Wednesday")


serviced_bus_stops.head()
# serviced_bus_stops = dt.filter_bus_timetable_by_date(bus_timetable_df, '20220822')
