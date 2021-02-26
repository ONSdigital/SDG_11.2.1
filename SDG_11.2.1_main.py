# Core imports
import os

# Third party imports
import geopandas as gpd
import matplotlib.pyplot as plt
import pandas as pd

# Module imports
from geospatial_mods import *
from data_ingest import *
from data_transform import *

# TODO: inventory check: why is get_and_save_geo_dataset not used

# Constants
default_crs = 'EPSG:27700'


# get current working directory
cwd = os.getcwd()

# define data directory
data_dir = (os.path.join
            (cwd,
             'data'))

# define url, paths and names related to zip download
zip_link = "http://naptan.app.dft.gov.uk/DataRequest/Naptan.ashx?format=csv"
zip_name = "Napatan.zip"
zip_path = os.path.join(data_dir, zip_name)
csv_nm = 'Stops.csv'
csv_path = os.path.join(data_dir, csv_nm)

# Download the zip file and extract the stops csv
_ = dl_csv_make_df(csv_nm,
                   csv_path,
                   zip_name,
                   zip_path,
                   zip_link,
                   data_dir)

# Create the geo dataframe with the stoppoly_from_polyss data
cols = ['NaptanCode', 'CommonName', 'Easting', 'Northing']

stops_geo_df = (geo_df_from_csv(path_to_csv=csv_path,
                                delim=',',
                                geom_x='Easting',
                                geom_y='Northing',
                                cols=cols,
                                crs=default_crs))

# # Getting the Lower Super Output Area for the UK into a dataframe
uk_LSOA_shp_file = "Lower_Layer_Super_Output_Areas__December_2011__Boundaries_EW_BGC.shp"
full_path = os.path.join(os.getcwd(), "data", "LSOA_shp", uk_LSOA_shp_file)
uk_LSOA_df = geo_df_from_geospatialfile(path_to_file=full_path)

# Get a polygon of Birmingham based on the Location Code
just_birmingham_poly = (get_polygons_of_loccode(
    geo_df=uk_LSOA_df, dissolveby='LSOA11NM', search="Birmingham"))

# Creating a Geo Dataframe of only stops in Birmingham

birmingham_stops_geo_df = (find_points_in_poly
                           (geo_df=stops_geo_df,
                            polygon_obj=just_birmingham_poly))

# Getting the west midlands population
Wmids_pop_df = pd.read_csv(os.path.join
                           (data_dir,
                            'population_estimates',
                            'westmids_pop_only.csv'))

# Get population weighted centroids into a dataframe
uk_pop_wtd_centr_df = (geo_df_from_geospatialfile
                       (os.path.join
                        (data_dir,
                         'pop_weighted_centroids')))

# Get output area boundaries
OA_df = pd.read_csv('https://opendata.arcgis.com/datasets/7763a773b61445128ed3251e27be5139_0.csv?outSR=%7B%22latestWkid%22%3A27700%2C%22wkid%22%3A27700%7D')

# Merge with uk population df
uk_pop_wtd_centr_df = uk_pop_wtd_centr_df.merge(OA_df, on="OA11CD", how='left')

# Clean after merge
uk_pop_wtd_centr_df.drop('OBJECTID_y', axis=1, inplace=True)
uk_pop_wtd_centr_df.rename({'OBJECTID_x': 'OBJECTID'}, inplace=True)

# Getting the urban-rural classification by OA for England and Wales
zip_link = "https://www.arcgis.com/sharing/rest/content/items/3ce248e9651f4dc094f84a4c5de18655/data"
zip_name = "RUC11_OA11_EW.zip"
zip_path = os.path.join(data_dir, zip_name)
csv_nm = 'RUC11_OA11_EW.csv'
csv_path = os.path.join(data_dir, csv_nm)
_ = dl_csv_make_df(csv_nm, csv_path, zip_name, zip_path, zip_link, data_dir)
# Make a df of the urban-rural classification
urb_rur_df = pd.read_csv(csv_path)

# These are the codes (RUC11CD) mapping to rural and urban descriptions (RUC11)
# I could make this more succinct, but leaving here for clarity and maintainability
urban_dictionary = {'A1': 'Urban major conurbation', 
                    'C1': 'Urban city and town', 
                    'B1': 'Urban minor conurbation',
                    'C2': 'Urban city and town in a sparse setting'}
rural_dictionary = {'F1': 'Rural hamlets and isolated dwellings',
                    'E1': 'Rural village',
                    'D1': 'Rural town and fringe',
                    'E2': 'Rural village in a sparse setting',
                    'F2': 'Rural hamlets and isolated dwellings in a sparse setting',
                    'D2': 'Rural town and fringe in a sparse setting'}

# mapping to a simple urban or rural classification
urb_rur_df["urb_rur_class"] =  (urb_rur_df.RUC11CD.map
                                (lambda x: "urban" 
                                if x in urban_dictionary.keys() 
                                else "rural"))
# filter the df. We only want OA11CD and an urban/rurual classification
urb_rur_df = urb_rur_df[['OA11CD','urb_rur_class']]

# joining urban rural classification onto the pop df
uk_pop_wtd_centr_df = uk_pop_wtd_centr_df.merge(urb_rur_df, on="OA11CD", how='left')

# Joining the West Mids population dataframe to the centroids dataframe, #forthedemo
Wmids_pop_df = Wmids_pop_df.join(
    other=uk_pop_wtd_centr_df.set_index('OA11CD'), on='OA11CD', how='left')

# Make B'ham LSOA just #forthedemo
bham_LSOA_df = uk_LSOA_df[uk_LSOA_df.LSOA11NM.str.contains("Birmingham")] 
bham_LSOA_df = bham_LSOA_df[['LSOA11CD', 'LSOA11NM', 'geometry']]

# merge the two dataframes limiting to just Birmingham #forthedemo
bham_pop_df = Wmids_pop_df.merge(bham_LSOA_df,
                                 how='right',
                                 left_on='LSOA11CD',
                                 right_on='LSOA11CD',
                                 suffixes=('_pop', '_LSOA'))
# rename the "All Ages" column to pop_count as it's the population count 
bham_pop_df.rename(columns={"All Ages": "pop_count"}, inplace=True)

# change pop_count to number (int)
bham_pop_df['pop_count'] = pd.to_numeric(bham_pop_df.pop_count.str.replace(",", ""))

# Generate a list of ages 
age_lst = gen_age_col_lst()

# Get a datframe limited to the data ages columns only
age_df = slice_age_df(bham_pop_df, age_lst)

# Create a list of tuples of the start and finish indexes for the age bins
age_bins = get_col_bins(age_lst)

# get the ages in the age_df binned, and drop the original columns
age_df = bin_pop_ages(age_df, age_bins, age_lst)

# Ridding the bham pop df of the same cols
bham_pop_df.drop(age_lst, axis=1, inplace=True)

# merging summed+grouped ages back in
bham_pop_df = pd.merge(bham_pop_df, age_df, left_index=True, right_index=True)
# converting into GeoDataFrame
bham_pop_df = gpd.GeoDataFrame(bham_pop_df)

# create a buffer around the stops, in column "geometry" #forthedemo
# the `buffer_points` function changes the df in situ
_ = buffer_points(birmingham_stops_geo_df)
# TODO: Ask DataScience people why this is changed in situ

# renaming the column to geometry so the point in polygon func gets expected
bham_pop_df.rename(columns = {"geometry_pop": "geometry"}, inplace=True)

# find all the pop centroids which are in the bham_stops_poly
pop_in_poly_df = find_points_in_poly(bham_pop_df, birmingham_stops_geo_df)

# TODO: pop_in_poly_df has a lot of duplicates. Find out why
# Dropping duplicates
pop_in_poly_df.drop_duplicates(inplace=True)


# Count the population served by public transport
served = pop_in_poly_df.pop_count.count()
full_pop = bham_pop_df.pop_count.count()
not_served = full_pop - served

print(f"""The number of people who are served by public transport is {served}. \n 
        The full population of Birmingham is calculated as {full_pop}
        While the number of people who are not served is {not_served}""")

# Plot all the buffered stops in B'ham and AG on to a map #forthedemo
# fig, ax = plt.subplots()
# p = gpd.GeoSeries(pop_in_poly_df)
# p.plot(ax=ax)
# plt.show()
# fig.savefig('bham_ag_stops.png') 