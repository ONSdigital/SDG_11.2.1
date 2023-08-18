# Core imports
import os
import sys

# Third party imports
import pandas as pd
import geopandas as gpd
import yaml

# add the parent directory to the path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# Module imports
import data_ingest as di
import data_transform as dt
import geospatial_mods as gs

# get current working directory
CWD = os.getcwd()

# Load config
with open(os.path.join(CWD, "config.yaml"), encoding="utf-8") as yamlfile:
    config = yaml.load(yamlfile, Loader=yaml.FullLoader)
    module = os.path.basename(__file__)
    print(f"Config loaded in {module}")

# Constants
DEFAULT_CRS = config["default_crs"]
DATA_DIR = config["data_dir"]
BUS_IN_DIR = config['bus_in_dir']
TRAIN_IN_DIR = config['train_in_dir']
URB_RUR_ZIP_LINK = config["urb_rur_zip_link"]
URB_RUR_TYPES = config["urb_rur_types"]
ENG_WALES_PREPROCESSED_OUTPUT = config["eng_wales_preprocessed_output"]

# Years
CALCULATION_YEAR = str(config["calculation_year"])
CENTROID_YEAR = str(config["centroid_year"])
EW_OA_LOOKUP_YEAR = str(config["ew_oa_lookup_year"])
POP_YEAR = str(config["population_year"])


# ---------
# Load and process stops data
# ---------------------------
print('Processing stops data')

# Highly serviced bus and train stops created from SDG_bus_timetable
# and SDG_train_timetable for england, wales and scotland.
# Metros and trains added from NAPTAN as we dont have timetable
# data for these stops. Hence, they wont be highly serviced.

highly_serviced_bus_stops = di.feath_to_df('bus_highly_serviced_stops',
                                            BUS_IN_DIR)
highly_serviced_train_stops = di.feath_to_df('train_highly_serviced_stops',
                                              TRAIN_IN_DIR)

# Get Tram data
naptan_df = di.get_stops_file(url=config["naptan_api"],
                              dir=os.path.join("data",
                                               "stops"))

# Isolating the tram and metro stops
tram_metro_stops = naptan_df[naptan_df.StopType.isin(["PLT", "MET", "TMU"])]

# Take only active, pending or new stops
tram_metro_stops = (
    tram_metro_stops[tram_metro_stops['Status'].isin(['active',
                                                      'pending',
                                                      'new'])]
)

# Add a column for transport mode
tram_metro_stops['transport_mode'] = 'tram_metro'
highly_serviced_bus_stops['transport_mode'] = 'bus'
highly_serviced_train_stops['transport_mode'] = 'train'

# Add a column for stop capacity type
# Buses are low capcity
# Trains, trams and metros are high capacity
tram_metro_stops['capacity_type'] = 'high'
highly_serviced_bus_stops['capacity_type'] = 'low'
highly_serviced_train_stops['capacity_type'] = 'high'

# Standardise dataset columns for union
column_renamer = {"NaptanCode": "station_code",
                  "Easting": "easting",
                  "Northing": "northing"}

column_filter = ["station_code", "easting", "northing",
                 "transport_mode", "capacity_type"]

tram_metro_stops.rename(columns=column_renamer, inplace=True)
tram_metro_stops = tram_metro_stops[column_filter]

highly_serviced_bus_stops.rename(columns=column_renamer, inplace=True)
highly_serviced_bus_stops = highly_serviced_bus_stops[column_filter]

highly_serviced_train_stops.rename(columns=column_renamer, inplace=True)
highly_serviced_train_stops = highly_serviced_bus_stops[column_filter]

# Merge into one dataframe
dfs_to_combine = [highly_serviced_bus_stops,
                  highly_serviced_train_stops,
                  tram_metro_stops]

filtered_stops_df = pd.concat(dfs_to_combine)

# Convert to geopandas df
stops_geo_df = (gs.geo_df_from_pd_df(pd_df=filtered_stops_df,
                                     geom_x='easting',
                                     geom_y='northing',
                                     crs=DEFAULT_CRS))

# Export dataset to geojson
path = os.path.join(ENG_WALES_PREPROCESSED_OUTPUT, 'stops_geo_df.geojson')
stops_geo_df.to_file(path, driver='GeoJSON', index=False)

# -------------------------------------
# Load and process local authority data
# -------------------------------------
print('Processing local authority data')

# Local authority shapefile covers the UK so extracting just shapefile
# for england, wales and scotland.
# Note that local authorities in scotland are commonly knows as councils.

# download shapefiles if switch set to cloud
file_path_to_get = os.path.join("data", "LA_shp", CALCULATION_YEAR)
di.download_data(file_path_to_get)

# Getting path for LA shapefile
uk_la_path = di.get_shp_abs_path(dir=os.path.join("data",
                                                  "LA_shp",
                                                  CALCULATION_YEAR))

# Create geopandas dataframe from the shapefile
uk_la_file = di.geo_df_from_geospatialfile(path_to_file=uk_la_path)

# Filter for just england and wales
lad_code_col = f'LAD{CALCULATION_YEAR[-2:]}CD'

ew_la_df = (
    uk_la_file[uk_la_file[lad_code_col].str.startswith(('E', 'W'))]
)

# Keep only required columns
ew_la_df = (
    ew_la_df[[lad_code_col, f'LAD{CALCULATION_YEAR[-2:]}NM', 'geometry']])

# Export dataset to geojson
path = os.path.join(ENG_WALES_PREPROCESSED_OUTPUT, 'ew_la_df.geojson')
ew_la_df.to_file(path, driver='GeoJSON', index=False)

# ------------------------------------------------------
# Load and process local authority to output area lookup
# ------------------------------------------------------
print('Processing local authority to output area lookup')

lad_name_col = f'LAD{EW_OA_LOOKUP_YEAR[-2:]}NM'

ew_oa_la_lookup_path = di.get_oa_la_csv_abspath(
    os.path.join("data", "oa_la_mapping", EW_OA_LOOKUP_YEAR))

ew_oa_la_lookup_df = pd.read_csv(di.path_or_url(ew_oa_la_lookup_path),
                                 usecols=["OA11CD", lad_name_col])


# ---------------------------------------
# Load and process output area boundaries
# ---------------------------------------
print('Processing output area boundaries')

ew_oa_boundaries_df = pd.read_csv(
        di.path_or_url(os.path.join("data",
                     "Output_Areas__December_2011__Boundaries_EW_BGC.csv")))

# Restrict to just required columns
ew_oa_boundaries_df = ew_oa_boundaries_df[['OA11CD', 'LAD11CD']]


# --------------------------------
# Load and process population data
# --------------------------------
print('Processing population data')

# Get list of all pop_estimate files for target year
ew_pop_files = os.listdir(os.path.join("data",
                                       "population_estimates",
                                       POP_YEAR))

# Get the population data for the whole nation for the specified year
ew_pop_df = di.get_whole_nation_pop_df(ew_pop_files, POP_YEAR)

# Keep only required columns
ew_pop_df = ew_pop_df.drop(['LSOA11CD_x', 'LSOA11CD_y', 'LSOA11CD'], axis=1)

# Group and reformat age data
# Get a list of ages from config
age_lst = config['age_lst']

# Get a datframe limited to the data ages columns only
ew_age_df = dt.slice_age_df(ew_pop_df, age_lst)

# Create a list of tuples of the start and finish indexes for the age bins
age_bins = dt.get_col_bins(age_lst)

# get the ages in the age_df binned, and drop the original columns
ew_age_df = dt.bin_pop_ages(ew_age_df, age_bins, age_lst)

# Remove the same columns from original population data
ew_pop_df.drop(age_lst, axis=1, inplace=True)

# merging summed and grouped ages back into population df
ew_pop_df = pd.merge(ew_pop_df, ew_age_df, left_index=True, right_index=True)


# -----------------------------------------------
# Load and process popualation weighted centroids
# -----------------------------------------------
print('Processing population weighted centroids')

file_path_to_get = os.path.join("data",
                                'pop_weighted_centroids',
                                CENTROID_YEAR)
di.download_data(file_path_to_get)

ew_pop_wtd_centr_df = (di.geo_df_from_geospatialfile(
    os.path.join(DATA_DIR, 'pop_weighted_centroids', CENTROID_YEAR)))

# Keep required columns
ew_pop_wtd_centr_df = ew_pop_wtd_centr_df[['OA11CD', 'geometry']]


# --------------------------------
# Load and process disability data
# --------------------------------
print('Processing disability data')

ew_disability_df = pd.read_csv(di.path_or_url(
    os.path.join("data", "disability_status", "nomis_QS303.csv")),
    header=5)

# drop the column "mnemonic" as it seems to be a duplicate of the OA code
# also "All categories: Long-term health problem or disability" is not needed,
# nor is "Day-to-day activities not limited"
drop_lst = ["mnemonic",
            "All categories: Long-term health problem or disability"]
ew_disability_df.drop(drop_lst, axis=1, inplace=True)

# the col headers are database unfriendly. Defining their replacement names
replacements = {"2011 output area": 'OA11CD',
                "Day-to-day activities limited a lot": "disab_ltd_lot",
                "Day-to-day activities limited a little": "disab_ltd_little",
                'Day-to-day activities not limited': "disab_not_ltd"}

# renaming the dodgy col names with their replacements
ew_disability_df.rename(columns=replacements, inplace=True)

# Export dataset to feather file
path = os.path.join(ENG_WALES_PREPROCESSED_OUTPUT, 'ew_disability_df.feather')
ew_disability_df.to_feather(path)


# -------------------------
# Load and process RUC data
# -------------------------
print('Processing rural urban classification data')

ew_urb_rur_df = pd.read_csv(os.path.join('data', 'RUC11_OA11_EW.csv'), 
                            dtype={'OA11CD':'str', 'RU11CD':'category'})

# These are the codes (RUC11CD) mapping to rural and urban descriptions (RUC11)
# I could make this more succinct, but leaving here
# for clarity and maintainability
urban_dictionary = {'A1': 'Urban major conurbation',
                    'C1': 'Urban city and town',
                    'B1': 'Urban minor conurbation',
                    'C2': 'Urban city and town in a sparse setting'}

# mapping to a simple urban or rural classification
ew_urb_rur_df["urb_rur_class"] = (
    ew_urb_rur_df.RUC11CD.map(lambda x: "urban"
                              if x in urban_dictionary.keys()
                              else "rural"))

# filter the df. We only want OA11CD and an urban/rurual classification
ew_urb_rur_df = ew_urb_rur_df[['OA11CD', 'urb_rur_class']]


# -----------------------------------
# Merge relevant datasets in the PWCs
# -----------------------------------
print('Creating final preprocessed dataset')

# Merge datasets into PWCs ready for analysis
lad_col = f'LAD{CALCULATION_YEAR[-2:]}NM'

# 1 output area boundaries
ew_df = ew_pop_wtd_centr_df.merge(
    ew_oa_boundaries_df, on="OA11CD", how='left')

# 2 Rural urban classification
ew_df = ew_df.merge(ew_urb_rur_df, on="OA11CD", how='left')

# 3 Population data
# First join output area lookup onto population data
ew_pop_df = ew_pop_df.merge(ew_oa_la_lookup_df, on='OA11CD', how='left')
# Then add into PWC
ew_df = ew_df.join(ew_pop_df.set_index('OA11CD'), on='OA11CD', how='left')

# 4 local authority boundaries
# Drop uneeded geometry
ew_df = ew_df.merge(ew_la_df, how='right', left_on=lad_col,
                    right_on=lad_col, suffixes=('_pop', '_la'))
ew_df.drop(labels='geometry_la', axis=1, inplace=True)

# Convert to geopandas df and drop uneeded geometry
ew_df = gpd.GeoDataFrame(ew_df, geometry='geometry_pop', crs=DEFAULT_CRS)

# Export dataset to geojson
path = os.path.join(ENG_WALES_PREPROCESSED_OUTPUT, 'ew_df.geojson')
ew_df.to_file(path, driver='GeoJSON', index=False)
