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
required_files = ['stop_times', 'trips', 'calendar']

# Download link for zip
# Name of zip changes each day so will need to scrape the page to extract
# the latest
# As such, providing hard-coding filepaths for the time being
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
        if line.startswith('A'):
            station_name = line[5:31].strip()
            tiploc_code = line[36:43].strip()
            crs_code = line[49:52].strip()

            # Create a new datafrme to concatenate to the original
            new_df = pd.DataFrame([[station_name, tiploc_code, crs_code]], columns = msn_df.keys())
            msn_df = pd.concat([msn_df, new_df], ignore_index=True)


# Data contains alot more records than other sources of station names
# Also contains duplicates in the crs_code
# However tiploc_code is unique
msn_tiploc_unique = msn_df['tiploc_code'].is_unique
msn_crs_unique = msn_df['crs_code'].is_unique

# Think it makes sense to remove the duplicates in crs_code
msn_df = msn_df.drop_duplicates(subset = ['crs_code'])

# Attache the coordinates from the json
# At the moment using the original data from the github repo (linked in the issue)
# https://github.com/ellcom/UK-Train-Station-Locations/blob/master/uk-train-stations-dictonary.json
# James has reviewed and made amendments to this, so can use the latest when
# available.

# NOTE: Will need adding to cloud storage if we keep this method of
# attaching coordinates.
station_locations = os.path.join(output_directory, 'station_locations.csv')
station_locations_df = pd.read_csv(station_locations, usecols = ['station_code', 'latitude', 'longitude'])

# Join coordinates onto msn data

# left join to master station names and see which ones dont have lat and long
msn_data = pd.merge(msn_df, station_locations_df, how = 'left',
                        left_on = 'crs_code', right_on='station_code')
msn_data = msn_data[['station_name', 'tiploc_code', 'crs_code', 'latitude', 'longitude']]

# There are quite a few empty coords as the msn data contained alot
# more records.
no_coords = msn_data[msn_data['latitude'].isna()]

# Remove these from the data for the time being
msn_data = msn_data.dropna(subset=['latitude', 'longitude'], how='any')


# Extract mca data
#-----------------

# Basic idea which is massively simplified. Need to double check we are not
# omitting anything really important.
# Each new journey starts with BS. Within this journey we have
# multiple stops
# LO is origin
# LI are inbetween stops
# LT is terminating stop
# Then a new jounrey starts with BS again
# Within each journey are a few more lines that I beleieve we can ignore
# BX = extra details of the jounrey
# CR = changes en route. Doesnt contain any arrival / departure times.

# Create dataframe with specified schema
mca_df = pd.DataFrame({'tiploc_code': pd.Series(dtype = 'str'),
                       'arrival_time': pd.Series(dtype='str'),
                       'calender': pd.Series(dtype='str')})

# Create a flag that specifies when we have found a new journey
journey = False

with open(mca_file, 'r') as mca_data:
    # Skip the header
    next(mca_data)
    for line in mca_data:
        # A schedule is started by a record beginning with BS.
        # Other entries exist but are not needed for our purpose.

        # Schedules are then further broken down by transaction type 
        # (N - new, R - revised, D - delete)
        # Remove any that are transaction type delete.
        if line[0:3] == 'BSN' or line[0:3] == 'BSR':
            # Switch flag on as we have found a jounrey
            journey = True
            # Extract start and end date of service
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

        if journey:
            if line[0:2] == 'LO' or line[0:2] == 'LI':
                # Extract tiploc_code
                tiploc_code = line[2:10].strip()
                # Extract departure time for journey origin station
                if line[10:15].strip() != '':
                    departure_time = line[10:15]
                else:
                    departure_time = line[15:20]
            elif line[0:2] == 'LT':
                # Extract tiploc_code
                tiploc_code = line[2:10].strip()
                # Extract departure time
                if line[15:20].strip() != '':
                    departure_time = line[15:20]
                else:
                    departure_time = line[29:33]
                
                # As we know that LT signifies the last stop in a journey
                # and we have extracted everything we need we now switch the flag off
                journey = False
            else:
                # Skipping BR and CX
                continue 
            
            # Ceate a new data frame for each station in this journey, adding
            # the calender days into it.
            new_df = pd.DataFrame([[tiploc_code, departure_time, start_date, end_date, calender]],
                                    columns = mca_df.keys())
            mca_df = pd.concat([mca_df, new_df], ignore_index=True)


# Filter the MCA data
# -------------------

# Remove records that dont contain a departure time.
# We are not trying to add a time from either public scheduled / arrival time
# or scheduled pass time as not relevant.




# Only keep LI records where activity is T (stops to take up and set down
# passengers). All other activity types unsuitable.
# Do group by to see counts for different permutations to understand
# what we would be losing.


# Round up or down where time contains H (which means a half minute)