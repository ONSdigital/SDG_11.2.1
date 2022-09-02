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
train_timetable_zip_link = config["ENG_train_timetable_data"]
train_dataset_name = 'trains' # changes each time so using holding name
output_directory = os.path.join(CWD, 'data', 'england_train_timetable')
zip_path = os.path.join(output_directory, train_dataset_name)

# Download link for zip
# Name of zip changes each day so will need to scrape the page to extract
# the latest
# As such, providing hard-coding filepaths for the time being, and
# a shortened mca file just for proof of concept.
msn_file = os.path.join(output_directory, 'ttisf467.msn')
mca_file = os.path.join(output_directory, 'ttisf467_short.mca')


# Below code can be used once we have the latest filename
#di._grab_zip(file_nm=train_dataset_name,
#            zip_link=train_timetable_zip_link,
#            zip_path=zip_path)

#di._extract_zip(file_nm=train_dataset_name,
#                csv_nm='ttisf467.msn',
#                zip_path=zip_path,
#                csv_path=output_directory)


# Extract msn data
#-----------------

# Create dataframe with specified schema
msn_df = pd.DataFrame({'station_name': pd.Series(dtype = 'str'),
                       'tiploc_code': pd.Series(dtype='category'),
                       'crs_code': pd.Series(dtype='category')})

with open(msn_file, 'r') as msn_data:
    # Skip header
    next(msn_data)
    for line in msn_data:
        
        # Only interested in rows starting with A.
        # Rows starting with L display aliases of station names
        # Stripping the values because some are padded out with blank spaces
        # as part of the file format.

        # Coordinate data provided is actually the grid reference
        # but without the 100km square (two letters at the start) so
        # very difficult to extract coordinates. Hence, will add in
        # coordinate data from an external source.

        # NB tiploc_code is unique, but crs_code isnt.
        if line.startswith('A'):
            station_name = line[5:31].strip()
            tiploc_code = line[36:43].strip()
            crs_code = line[49:52].strip()

            # Create a new datafrme to concatenate to the original
            new_df = pd.DataFrame([[station_name, tiploc_code, crs_code]], columns = msn_df.keys())
            msn_df = pd.concat([msn_df, new_df], ignore_index=True)


# Remove the duplicates in crs_code
msn_df = msn_df.drop_duplicates(subset = ['crs_code'])

# Attach the coordinates for each train station
station_locations = os.path.join(output_directory, 'station_locations.csv')
station_locations_df = pd.read_csv(station_locations, usecols = ['station_code', 'latitude', 'longitude'])

# Join coordinates onto msn data
# left join to master station names and see which ones dont have lat and long
msn_data = pd.merge(msn_df, station_locations_df, how = 'left',
                        left_on = 'crs_code', right_on='station_code')
msn_data = msn_data[['station_name', 'tiploc_code', 'crs_code', 'latitude', 'longitude']]

# Remove stations with no coordinates
msn_data = msn_data.dropna(subset=['latitude', 'longitude'], how='any')


# Extract mca data
#-----------------

# Each new journey starts with BS. Within this journey we have
# multiple stops
# LO is origin
# LI are inbetween stops
# LT is terminating stop
# Then a new journey starts with BS again
# Within each journey are a few more lines that we can ignore e.g.
# BX = extra details of the journey
# CR = changes en route. Doesnt contain any arrival / departure times.

# Create dataframe with specified schema
mca_df = pd.DataFrame({'tiploc_code': pd.Series(dtype = 'str'),
                       'departure_time': pd.Series(dtype='str'),
                       'calender': pd.Series(dtype='str'),
                       'start_date': pd.Series(dtype='str'),
                       'end_date': pd.Series(dtype='str'),
                       'activity_type': pd.Series(dtype='str')})

# Create a flag that specifies when we have found a new journey
journey = False

# Create a flag that specifies we have found a departure time
time = False

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
            # Switch flag on as we have found a jounrey
            journey = True
            # Extract start and end date of service (yymmdd)
            start_date = line[9:15].strip()
            end_date = line[15:21].strip()
            # Extract the calender information for this journey
            # i.e. what days of the week it runs
            calender = line[21:28].strip()
            # Skip as this is all we need to do for an entry starting
            # with BS
            continue 

        # If we have found a new journey, go through the lines that are
        # part of this jounrey and extract relevant information.

        # Journeys split into origin (LO), stops (LI) and terminus (LT)
        # CR (changes en route) and BX (extra schedule details) can be
        # ignored as not relevant to our purpose.

        # If station has a departure time set the time flag to True
        # and extract time, tiploc_code and activity type.

        # NB if departure time doesnt exist we are not the trying to extract
        # from either public scheduled / arrival time or scheduled pass time 
        # as not relevant.

        # NB times can end on a H sometimes which indicates a half minute
        # Rather than rounding up and down, just ignoring this for the moment
        # and taking only first four characters (hh:mm)

        # NB Only keep LI records where the activity_type contains T (stops to
        # take up and set down passengers). All other activity types unsuitable.



        if journey:
            if line[0:2] == 'LO':
                if line[10:15].strip() != '':
                    time = True
                    # Extract departure time
                    departure_time = line[10:14].strip()
                    # Extract tiploc_code
                    tiploc_code = line[2:10].strip()
                    # Extract activity type
                    activity_type = line[29:41].strip()
            elif line[0:2] == 'LI':
                # If station has a departure time set the flag to True
                # and extract time, tiploc_code and activity type
                if line[15:20].strip() != '':
                    time = True
                    # Extract departure time
                    departure_time = line[15:19].strip()
                    # Extract tiploc_code
                    tiploc_code = line[2:10].strip()
                    # Extract activity type if it contains T
                    # Activity type made up of upto 6 types
                    # each 2 characters so search for 'T '
                    if 'T ' in line[42:54]:
                        activity_type = line[42:54].strip()
            elif line[0:2] == 'LT':
                # If station has a departure time set the flag to True
                # and extract time, tiploc_code and activity type
                if line[15:20].strip() != '':
                    time = True
                    # Extract departure time
                    departure_time = line[15:19].strip()
                    # Extract tiploc_code
                    tiploc_code = line[2:10].strip()
                    # Extract acitivity type
                    activity_type = line[25:37].strip()

                # As we know that LT signifies the last stop in a journey
                # and we have extracted everything we need we now switch the flag off
                journey = False
            else:
                # Skipping BR and CX
                continue 
            
            # If time flag has been switched on, then create a new data frame for 
            # this stop, adding the calender days into it.
            if time:
                new_df = pd.DataFrame([[tiploc_code, departure_time,
                                        calender, start_date, end_date, activity_type]],
                                        columns = mca_df.keys())
                mca_df = pd.concat([mca_df, new_df], ignore_index=True)

            # Reset time flag for new line
            time = False
