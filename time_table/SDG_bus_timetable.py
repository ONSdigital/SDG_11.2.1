# core
import os
import sys

# third party
import yaml
import pandas as pd

# Getting the parent directory of the current file
current = os.path.dirname(os.path.realpath(__file__))
parent = os.path.dirname(current)
# Appending to path so that we can import modules from the src folder
sys.path.append(parent)

# Our modules
import src.data_ingest as di
import src.data_transform as dt

# Get current working directory
CWD = os.getcwd()

# Load config
with open(os.path.join(CWD, "config.yaml")) as yamlfile:
    config = yaml.load(yamlfile, Loader=yaml.FullLoader)
    module = os.path.basename(__file__)
    print(f"Config loaded in {module}")

# Parameters
bus_timetable_zip_link = config["ENG_bus_timetable_data"]
bus_dataset_name = 'itm_all_gtfs'
bus_data_output_dir = os.path.join(CWD, 'data', 'england_bus_timetable')
zip_path = os.path.join(bus_data_output_dir, bus_dataset_name)
required_files = ['stop_times', 'trips', 'calendar']
auto_download_bus = config["auto_download_bus"]
timetable_day = config["timetable_day"]
day_filter_type = config["day_filter"]
early_timetable_hour = config["early_timetable_hour"]
late_timetable_hour = config["late_timetable_hour"]

# Calculate if bus timetable needs to be downloaded.
# If current folder doesnt exist, or hasnt been modified then
# flag to be downloaded

files_to_check = [f"{file}.txt" for file in required_files]
paths_to_check = [os.path.join(bus_data_output_dir, file)
                  for file in files_to_check]
each_file_checked = [di._persistent_exists(path) for path in paths_to_check]

if not all(each_file_checked):
    try:
        os.makedirs(bus_data_output_dir)
    except FileExistsError:
        print(f"Directory {bus_data_output_dir} already exists")
    download_bus_timetable = True
else:
    # Find when the last download occured
    # If > 7 days ago, download the data again
    download_bus_timetable = di.best_before(path=bus_data_output_dir,
                                            number_of_days=7)

# ---------------------------
# Download bus timetable data
# ---------------------------

# Note downloads if flag above, and flag in config, set as True.
# Using individual data ingest functions (rather than
# import_extract_delete_zip) as files are .txt not .csv.
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
                        csv_path=bus_data_output_dir)

    # Remove zip file
    di._delete_junk(file_nm=bus_dataset_name,
                    zip_path=zip_path)

# ------------------------------------------
# Load the text files into pandas dataframes
# ------------------------------------------

# Specify dtypes to remove unwanted columns and allow loading due to
# mixed data types in certain columns.
# Cannot parse datetime dtypes, and current function doesnt allow parse_dates
# parameter in read_csv. So reading in as object type for the time being.
# Dont think they need to be datetime format as we are using every trip
# regardless of the date, and just want to extract the HH from the date which
# can be done from a string.

# Stop times
feath_ = os.path.join(bus_data_output_dir, "stop_times.feather")
if os.path.exists(feath_):
    stop_times_df = di._feath_to_df("stop_times", feath_)
else:
    stop_times_types = {'trip_id': 'category',
                        'departure_time': 'object', 'stop_id': 'category'}

    stop_times_df = di._csv_to_df(file_nm='stop_times',
                                  csv_path=os.path.join(
                                      bus_data_output_dir, 'stop_times.txt'),
                                  dtypes=stop_times_types)

# trips
feath_ = os.path.join(bus_data_output_dir, "trips.feather")
if os.path.exists(feath_):
    trips_df = di._feath_to_df("trips", feath_)
else:
    trips_types = {'route_id': 'category',
                   'service_id': 'category', 'trip_id': 'category'}

    trips_df = di._csv_to_df(
        file_nm='trips',
        csv_path=os.path.join(
            bus_data_output_dir,
            'trips.txt'),
        dtypes=trips_types)

# calendar
# NOTE: Not adding in saturday and sunday columns for stops because
# we are only interested in weekday trips for highly serviced stops
feath_ = os.path.join(bus_data_output_dir, "calendar.feather")
if os.path.exists(feath_):
    calendar_df = di._feath_to_df("calendar", feath_)
else:
    calendar_types = {
        'service_id': 'category',
        'monday': 'int64',
        'tuesday': 'int64',
        'wednesday': 'int64',
        'thursday': 'int64',
        'friday': 'int64',
        'start_date': 'object',
        'end_date': 'object'}

    calendar_df = di._csv_to_df(file_nm='calendar',
                                csv_path=os.path.join(
                                    bus_data_output_dir, 'calendar.txt'),
                                dtypes=calendar_types)

# ----------
# Clean data
# ----------

# Some departure times are > 24:00 so need to be removed.
# This is done automatically by restricting times to hours used
# to define highly serviced stops

hour_range = range(early_timetable_hour, late_timetable_hour)
valid_hours = [f'0{i}' if i < 10 else f'{i}' for i in hour_range]

# Check departure times for any NA values
if stop_times_df['departure_time'].isna().any():
    print("There are NA values in departure_time column")
    # If there are any, remove them
    stop_times_df = stop_times_df.dropna(subset=['departure_time'])

# Filter stop times to only include valid hours
stop_times_df = stop_times_df[
    stop_times_df['departure_time'].str.startswith(tuple(valid_hours))]

# Convert start and end date to datetime format
calendar_df['start_date'] = pd.to_datetime(
    calendar_df['start_date'], format='%Y%m%d')
calendar_df['end_date'] = pd.to_datetime(
    calendar_df['end_date'], format='%Y%m%d')

# ----------------
# Join data frames
# ----------------

bus_timetable_df = (
    (stop_times_df.merge(trips_df, on='trip_id', how='left'))
    .merge(calendar_df, on='service_id', how='left')
)

# Remove columns no longer required
# NOTE: Route_id not used at all so could be removed at load
bus_timetable_df = bus_timetable_df.drop(
    columns=['trip_id', 'route_id'])  # 'service_id'


# ----------------------------
# Extract stops for chosen day
# ----------------------------

# Only interested in stops that are used on a certain day

if day_filter_type == "general":
    timetable_day = timetable_day.lower()
    serviced_bus_stops_df = (
        bus_timetable_df[bus_timetable_df[timetable_day] == 1]
    )
elif day_filter_type == "exact":
    timetable_day = timetable_day.capitalize()
    serviced_bus_stops = dt.filter_bus_timetable_by_day(
        bus_timetable_df, timetable_day.capitalize())
else:
    print("Error: input error on day filter setting.")

# -----------------------
# Find frequency of stops
# -----------------------

# Take just HH from departure time
serviced_bus_stops_df['departure_time'] = (
    serviced_bus_stops_df['departure_time'].str.slice(0, 2)
)

bus_frequencies_df = pd.pivot_table(data=serviced_bus_stops_df,
                                    values=timetable_day,
                                    index='stop_id',
                                    columns='departure_time',
                                    aggfunc=len,
                                    fill_value=0)


# -----------------------------
# Extract highly serviced stops
# -----------------------------

# Only keep those which have at least one service an hour
bus_highly_serviced_stops = bus_frequencies_df[(
    bus_frequencies_df > 0).all(axis=1)]

# Read in naptan data
stops_df = di.get_stops_file(url=config["NAPTAN_API"],
                             dir=os.path.join(os.getcwd(), "data", "stops"))

# Add easting and northing
bus_highly_serviced_stops = bus_highly_serviced_stops.merge(
    stops_df, how='inner', left_on='stop_id', right_on='ATCOCode')

# Remove stops that dont have coordinates
bus_highly_serviced_stops = bus_highly_serviced_stops.dropna(
    subset=['Easting', 'Northing'], how='any')

# Drop the hours columns
bus_highly_serviced_stops = (
    bus_highly_serviced_stops['NaptanCode', 'Easting', 'Northing']
)

# Save a copy to be ingested by SDG_11.2.1_main
bus_highly_serviced_stops.to_feather(os.path.join(
    bus_data_output_dir, 'bus_highly_serviced_stops.feather'))
bus_highly_serviced_stops.to_csv(
    os.path.join(
        bus_data_output_dir,
        'bus_highly_serviced_stops.csv'),
    index=False)
