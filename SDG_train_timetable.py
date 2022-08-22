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

# Download link for zip
# probably need to scrape webpage to get latest download available
# https://data.atoc.org/system/files/ttis467.zip

# Parameters
train_timetable_zip_link = config["ENG_train_timetable_data"]
train_dataset_name = 'trains' # changes each time so using holding name
output_directory = os.path.join(CWD, 'data', 'england_train_timetable')
zip_path = os.path.join(output_directory, train_dataset_name)
required_files = ['stop_times', 'trips', 'calendar']

""" di._grab_zip(file_nm=train_dataset_name,
            zip_link=train_timetable_zip_link,
            zip_path=zip_path)

di._extract_zip(file_nm=train_dataset_name,
                csv_nm='ttisf467.msn',
                zip_path=zip_path,
                csv_path=output_directory) """

msn_file = os.path.join(output_directory, 'ttisf467.msn')
msc_file = os.path.join(output_directory, 'ttisf467.msc')



# Create dataframe with specified schema
msn_df = pd.DataFrame({'station_name': pd.Series(dtype = 'str'),
                              'tiploc_code': pd.Series(dtype='str'),
                              'crs_code': pd.Series(dtype='float')})
with open(msn_file, 'r') as msn_data:
    next(msn_data)
    for line in msn_data:
        
        # Only interested in rows starting with A.
        # Rows starting with L display aliases of station names
        # Stripping the values because some are padded out with blank spaces
        # as part of the file format.
        if line.startswith('A'):
            station_name = line[5:31].strip()
            tiploc_code = line[36:43].strip()
            crs_code = line[49:52].strip()

            # CReate series to concat into df
            new_df = pd.DataFrame([[station_name, tiploc_code, crs_code]], columns = msn_df.keys())
            msn_df = pd.concat([msn_df, new_df], ignore_index=True)

# load and join to json
# find records which are not matching.


# CReate maps
import folium
import geopandas
from OSGridConverter import grid2latlong

l=grid2latlong('SU 15473 61790')

msn_df = msn_df[['easting', 'northing']]
shp = geopandas.GeoDataFrame(msn_df, geometry = geopandas.points_from_xy(msn_df.easting, msn_df.northing, crs = 'EPSG:27700'))
shp = shp.to_crs(epsg=4326)

m = folium.Map(location = [51.5072, 0.1276], zoom_start = 8)
    
geo_df_list = [[point.xy[1][0], point.xy[0][0]] for point in shp.geometry]
for coordinates in geo_df_list:
    m.add_child(folium.Marker(location = coordinates))

m.save('index.html')

# Tidy, where duplicates for name, reduce to one??
# convert to latitude and longitude