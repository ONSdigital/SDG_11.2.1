# core
import os
import sys
import logging
import pathlib as pl

# third party
import yaml
import pandas as pd

# # Getting the parent directory of the current file
current = os.path.dirname(os.path.realpath(__file__))
#parent = os.path.dirname(current)
# Appending to path so that we can import modules from the src folder
sys.path.append(current)

# Our modules
import time_table_utils as ttu # noqa E402
import data_transform as dt # noqa E402
import data_ingest as di # noqa E402

# Create logger
TrainLogger = logging.getLogger(__name__)
TrainLogger.setLevel(logging.INFO)

# Create a console handler
console_handler = logging.StreamHandler()
console_handler.setLevel(logging.INFO)

# Add the console handler to the logger
TrainLogger.addHandler(console_handler)

# get current working directory
CWD = os.getcwd()

# Load config
with open(os.path.join(CWD, "config.yaml")) as yamlfile:
    config = yaml.load(yamlfile, Loader=yaml.FullLoader)
    module = os.path.basename(__file__)
    print(f"Config loaded in {module}")


# Parameters
trn_data_output_dir = os.path.join('data', 'england_train_timetable')
station_locations = os.path.join(trn_data_output_dir,
                                 config['station_locations'])
msn_file = os.path.join(trn_data_output_dir, config["train_msn_filename"])
mca_file = os.path.join(trn_data_output_dir, config["train_mca_filename"])
day_filter_type = config["day_filter"]
timetable_day = config["timetable_day"]
early_timetable_hour = config["early_timetable_hour"]
late_timetable_hour = config["late_timetable_hour"]
required_files = ['stop_times', 'trips', 'calendar']
auto_download_train = config["auto_download_train"]

# Calculate if train timetable needs to be downloaded.
# If current folder doesnt exist, or hasnt been modified then
# flag to be downloaded

paths_to_check = [pl.Path(msn_file), pl.Path(mca_file)]
each_file_checked = [di.persistent_exists(path) for path in paths_to_check]

if not all(each_file_checked):
    try:
        di.make_non_existent_folder(trn_data_output_dir)
    except FileExistsError:
        print(f"Directory {trn_data_output_dir} already exists")
    download_train_timetable = True
else:
    # Find when the last download occured
    # If > 7 days ago, download the data again
    download_train_timetable = di.best_before(path=trn_data_output_dir,
                                            number_of_days=7)

# ---------------------------
# Download train timetable data
# ---------------------------

# Note downloads if flag above, and flag in config, set as True.
# Using individual data ingest functions (rather than
# import_extract_delete_zip) as files are .txt not .csv.
if download_train_timetable and auto_download_train:
    files_to_download = [file for file, boolean
                          in zip(paths_to_check, each_file_checked)
                            if not boolean]
    for file in files_to_download:
        di.download_shp_data(file)


# Extract msn data
msn_data = ttu.extract_msn_data(msn_file)

# Create dataframe from msn data
msn_df = pd.DataFrame(
    msn_data, columns=['station_name', 'tiploc_code', 'crs_code'])

# Clean msn data
# --------------
# Remove the duplicates in crs_code
msn_df = msn_df.drop_duplicates(subset=['crs_code'])

# Attach the coordinates for each train station
station_locations = os.path.join(trn_data_output_dir, 'station_locations.csv')
station_locations_df = pd.read_csv(
    di.path_or_url(station_locations), usecols=[
        'station_code', 'latitude', 'longitude'])

# Join coordinates onto msn data
# left join to master station names and see which ones dont have lat and long
msn_data_df = pd.merge(msn_df, station_locations_df, how='left',
                       left_on='crs_code', right_on='station_code')
msn_data_df = msn_data_df[['station_name', 'tiploc_code',
                           'crs_code', 'latitude', 'longitude']]

# Remove stations with no coordinates
msn_data_df = msn_data_df.dropna(subset=['latitude', 'longitude'], how='any')


schedules, stops = ttu.extract_mca(mca_file)


# Add data into dataframes
mca_schedule_df = pd.DataFrame(schedules,
                               columns=['schedule_id',
                                        'start_date',
                                        'end_date',
                                        'monday',
                                        'tuesday',
                                        'wednesday',
                                        'thursday',
                                        'friday'])

mca_stop_df = pd.DataFrame(stops,
                           columns=['schedule_id',
                                    'departure_time',
                                    'tiploc_code',
                                    'activity_type'])


# Clean data
# ----------

# Drop any duplicate schedule IDs
# As these are composed of an ID, start and end time, and calendar
# any duplicates we have must be duplicates in the dataset.
mca_schedule_df = mca_schedule_df.drop_duplicates(subset=['schedule_id'])

# Only keep trains with activity type T
# NB Only keep LI records where the activity_type contains T (stops to
# take up and set down passengers). All other activity types unsuitable.
mca_stop_df = mca_stop_df[mca_stop_df['activity_type'] == 'T']

# Only keep records with departure time between highly serviced hours
hour_range = range(early_timetable_hour, late_timetable_hour)
valid_hours = [f'0{i}' if i < 10 else f'{i}' for i in hour_range]

mca_stop_df = (
    mca_stop_df[
        mca_stop_df['departure_time'].str.startswith(tuple(valid_hours))]
)

# Convert start and end date to datetime format
mca_schedule_df['start_date'] = (
    pd.to_datetime(mca_schedule_df['start_date'], format='%y%m%d')
)
mca_schedule_df['end_date'] = (
    pd.to_datetime(mca_schedule_df['end_date'], format='%y%m%d')
)


# Join dataframes
# ---------------

train_timetable_df = (
    (mca_stop_df.merge(mca_schedule_df, on='schedule_id', how='left'))
    .merge(msn_df, on='tiploc_code', how='left')
)

# Remove columns no longer required
train_timetable_df = train_timetable_df.drop(columns=['schedule_id',
                                                      'activity_type',
                                                      'station_name'])

# Extract stops for chosen day
# ----------------------------

# Only interested in stops on a certain day
if day_filter_type == "general":
    timetable_day = timetable_day.lower()
    serviced_train_stops_df = (
        train_timetable_df[train_timetable_df[timetable_day] == 1]
    )
elif day_filter_type == "exact":
    timetable_day = timetable_day.capitalize()
    serviced_train_stops_df = (
        dt.filter_timetable_by_day(
            train_timetable_df, timetable_day.capitalize())
    )
else:
    print("Error: input error on day filter setting.")


# Find frequency of stops
# -----------------------

# Just take HH from departure time
serviced_train_stops_df['departure_time'] = (
    serviced_train_stops_df['departure_time'].str.slice(0, 2)
)

train_frequencies_df = pd.pivot_table(data=serviced_train_stops_df,
                                      values=timetable_day,
                                      index='tiploc_code',
                                      columns='departure_time',
                                      aggfunc=len,
                                      fill_value=0)


# Extract highly serviced stops
# -----------------------------

# Only keep stations with at least 1 service per hour
highly_serviced_train_stops_df = (
    train_frequencies_df[(train_frequencies_df > 0).all(axis=1)]
)

# Get the naptan data and limit to only the columns we need
naptan_df = di.get_stops_file(url=config["naptan_api"],
                              dir=os.path.join("data",
                                               "stops"))
# Create Tiploc column
naptan_df = dt.create_tiploc_col(naptan_df)

stations_df = naptan_df[naptan_df['StopType'] == 'RLY']
station_locations_df = stations_df[['Easting', 'Northing', 'tiploc_code']]


# Add easting and northing
highly_serviced_train_stops_df = (
    highly_serviced_train_stops_df.merge(station_locations_df,
                                         how='inner',
                                         on='tiploc_code')
)

# Remove stations with no coordinates
highly_serviced_train_stops_df = (
    highly_serviced_train_stops_df.dropna(subset=['Easting', 'Northing'],
                                          how='any')
)

# Keep only required columns in highly_serviced_train_stops_df
highly_serviced_train_stops_df = (
    highly_serviced_train_stops_df.drop(columns=valid_hours)
)

# Save a copy to be ingested into SDG_main
highly_serviced_train_stops_df.to_feather(
    os.path.join(trn_data_output_dir, 'train_highly_serviced_stops.feather'))

highly_serviced_train_stops_df.to_csv(
    os.path.join(trn_data_output_dir,
                 'train_highly_serviced_stops.csv'), index=False)

TrainLogger.info("Train timetable pipeline complete")
