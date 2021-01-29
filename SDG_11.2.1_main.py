# Core imports
import os

# Third party imports
import geopandas as gpd
import matplotlib.pyplot as plt
import pandas as pd
from shapely.ops import unary_union

# Module imports
from geospatial_mods import find_points_in_poly, buffer_points, get_polygons_of_loccode, poly_from_polys, ward_nrthng_eastng
from data_ingest import dl_csv_make_df, geo_df_from_csv, geo_df_from_geospatialfile
from data_transform import bin_pop_ages, gen_age_col_lst, get_col_bins, slice_age_df

# TODO: inventory check: why is get_and_save_geo_dataset not used

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
                                cols=cols))

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

# Joining the West Mids population dataframe to the centroids dataframe
Wmids_pop_df = Wmids_pop_df.join(
    other=uk_pop_wtd_centr_df.set_index('OA11CD'), on='OA11CD', how='left')

# Make B'ham LSOA
bham_LSOA_df = uk_LSOA_df[uk_LSOA_df.LSOA11NM.str.contains("Birmingham")]
bham_LSOA_df = bham_LSOA_df[['LSOA11CD', 'LSOA11NM', 'geometry']]

# merge the two dataframes limiting to just Birmingham
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

# create a buffer around the stops, in column "geometry"
# the `buffer_points` function changes the df in situ
_ = buffer_points(birmingham_stops_geo_df)
# TODO: Ask DataScience people why this is changed in situ

# grab some coordinates for a little section of Birmingham: Acocks Green

# Get the needed eastings and northings for Acocks Green for demo
mins_maxs= (ward_nrthng_eastng(district="E08000025",
                               ward="E05011118"))

# Filtering the stops to the ward of Acocks Green for demo
ag_stops_geo_df = (filter_stops_by_ward(birmingham_stops_geo_df, mins_maxs))

# Create the polygon for the combined buffered stops in Acocks Green for demo
ag_stops_poly = poly_from_polys(ag_stops_geo_df)

# Create the polygon for the combined buffered stops in B'ham
bham_stops_poly = poly_from_polys(birmingham_stops_geo_df)

# Plot all the buffered stops in B'ham and AG on to a map
fig, ax = plt.subplots()
p = gpd.GeoSeries(bham_stops_poly)

# Plot Acocks Green only stops
a = gpd.GeoSeries(ag_stops_poly)
p.plot(ax=ax)
a.plot(ax=ax, color='gold')
plt.show()
fig.savefig('bham_ag_stops.png') 