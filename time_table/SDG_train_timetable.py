# core
from main import naptan_df
import os

# third party
import yaml
import pandas as pd

# our modules
import data_transform as dt
import time_table_utils as ttu

# get current working directory
CWD = os.getcwd()

# Load config
with open(os.path.join(CWD, "config.yaml")) as yamlfile:
    config = yaml.load(yamlfile, Loader=yaml.FullLoader)
    module = os.path.basename(__file__)
    print(f"Config loaded in {module}")


# Parameters
trn_data_output_dir = os.path.join(CWD, 'data', 'england_train_timetable')
station_locations = os.path.join(trn_data_output_dir,
                                 config['station_locations'])
msn_file = os.path.join(trn_data_output_dir, config["train_msn_filename"])
mca_file = os.path.join(trn_data_output_dir, config["train_mca_filename"])
day_filter_type = config["day_filter"]
timetable_day = config["timetable_day"]
early_timetable_hour = config["early_timetable_hour"]
late_timetable_hour = config["late_timetable_hour"]

# Extract msn data
# -----------------



ttu.extract_msn_data(msn_file)



# Create dataframe
msn_df = pd.DataFrame(msn, columns=['station_name', 'tiploc_code', 'crs_code'])


# Clean msn data
# --------------

# Remove the duplicates in crs_code
msn_df = msn_df.drop_duplicates(subset=['crs_code'])

# Attach the coordinates for each train station
station_locations = os.path.join(trn_data_output_dir, 'station_locations.csv')
station_locations_df = pd.read_csv(
    station_locations, usecols=[
        'station_code', 'latitude', 'longitude'])

# Join coordinates onto msn data
# left join to master station names and see which ones dont have lat and long
msn_data = pd.merge(msn_df, station_locations_df, how='left',
                    left_on='crs_code', right_on='station_code')
msn_data = msn_data[['station_name', 'tiploc_code',
                     'crs_code', 'latitude', 'longitude']]

# Remove stations with no coordinates
msn_data = msn_data.dropna(subset=['latitude', 'longitude'], how='any')


# Extract mca data
# -----------------

# Each new journey starts with BS. Within this journey we have
# multiple stops
# LO is origin
# LI are inbetween stops
# LT is terminating stop
# Then a new journey starts with BS again
# Within each journey are a few more lines that we can ignore e.g.
# BX = extra details of the journey
# CR = changes en route. Doesnt contain any arrival / departure times.

# Create a flag that specifies when we have found a new journey
journey = False

# Store schedule information
schedules = []

# Store stop information
stops = []

# Start by finding all the schedules within the file. Extract relevant
# information into the journey dataframe, and then copy unique_id
# onto all trips within that journey.

with open(mca_file, 'r') as mca_data:
    # Skip the header
    next(mca_data)
    for line in mca_data:
        # A schedule is started by a record beginning with BS.
        # Other entries exist but are not needed for our purpose.

        # Schedules are then further broken down by transaction type
        # (N - new, R - revised, D - delete)
        # Ignore any that are transaction type delete.
        if line[0:3] == 'BSN' or line[0:3] == 'BSR':
            # Switch flag on as we have found a journey
            journey = True

            # Get unique ID for schedule
            # ID in dataset is not actually unique as same train has several
            # schedules with different dates and calendars. Create ID from
            # these variables.
            schedule_id = line[3:9] + line[9:15] + line[15:21] + line[21:28]

            # Extract start and end date of service (yymmdd)
            start_date = line[9:15].strip()
            end_date = line[15:21].strip()

            # Extract the calender information for this journey
            # i.e. what days of the week it runs
            # Only interested in weekdays for the timebeing.
            monday = int(line[21].strip())
            tuesday = int(line[22].strip())
            wednesday = int(line[23].strip())
            thursday = int(line[24].strip())
            friday = int(line[25].strip())

            # Store data to be added to the dataframe
            schedules.append([schedule_id,
                              start_date,
                              end_date,
                              monday,
                              tuesday,
                              wednesday,
                              thursday,
                              friday])

            # Skip as this is all we need to do for an entry starting
            # with BS
            continue

        # If we have found a new journey, go through the lines that are
        # part of this jounrey and give them the schedule id.

        # Journeys split into origin (LO), stops (LI) and terminus (LT)
        # CR (changes en route) and BX (extra schedule details) can be
        # ignored as not relevant to our purpose.

        # If station has a departure time extract time, tiploc_code and
        # activity type.

        # NB times can end on a H sometimes which indicates a half minute
        # Rather than rounding up and down, just ignoring this for the moment
        # and taking only first four characters (hh:mm)

        if journey:

            if line[0:2] == 'LO':

                departure_time = line[10:14].strip()
                tiploc_code = line[2:10].strip()
                activity_type = line[29:41].strip()

            elif line[0:2] == 'LI':

                departure_time = line[15:19].strip()
                tiploc_code = line[2:10].strip()
                activity_type = line[42:54].strip()

            elif line[0:2] == 'LT':

                departure_time = line[15:19].strip()
                tiploc_code = line[2:10].strip()
                activity_type = line[25:37].strip()

                # As we know that LT signifies the last stop in a journey
                # and we have extracted everything we need we now switch the
                # flag off
                journey = False
            else:
                # Skipping BR and CX
                continue

                # Store data to be added to dataframe
            stops.append([schedule_id,
                          departure_time,
                          tiploc_code,
                          activity_type])


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

# Remove the duplicates in crs_code
msn_df = msn_df.drop_duplicates(subset=['crs_code'])

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

# Read in station location data
# Attach the coordinates for each train station
# station_locations_df = pd.read_csv(station_locations,
#                                    usecols=['station_code',
#                                             'easting',
#                                             'northing'])
# Get the naptan data from main.py
# limit to only the columns we need
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
