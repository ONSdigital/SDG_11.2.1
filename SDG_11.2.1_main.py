# Core imports
import os

# Third party imports
import geopandas as gpd
import matplotlib.pyplot as plt
import pandas as pd
from shapely.ops import unary_union

# Module importss
from modules import (dl_csv_make_df, find_points_in_poly, geo_df_from_csv,
                     geo_df_from_geospatialfile,
                     buffer_points)

# TODO: inventory check: why is get_and_save_geo_dataset not used

# get current working directory
cwd = os.getcwd()

# define data directory
data_dir = (os.path.join
            (cwd,
             'data'))

# define some params related to zip download
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

# Create the geo dataframe with the stops data
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


# geo_df_from_geospatialfile(os.path.join
#                            (os.getcwd(),
#                             'data',
#                             'birmingham_geo_dataset.json'))

# # Manipulating the Birmingham df
# # splitting the "codes" column into "gss" and "unit_id"
# # setting "id" as the index

# birmingham_df = pd.DataFrame.from_dict(birmingahm_gsscode_dataset).T
# gss_code_cols = pd.DataFrame.from_dict(birmingahm_gsscode_dataset).T.codes.apply(pd.Series).drop("ons", axis=1)
# birmingham_df = birmingham_df.join(gss_code_cols).drop(["codes", "all_names"], axis=1)
# birmingham_df.set_index('id', inplace=True)


def get_polygons_of_loccode(geo_df, dissolveby='OA11CD', search=None):
    """
    Gets the polygon for a place based on it name, LSOA code or OA code

    Parameters:
    geo_df: (gpd.Datafame):
    loc_code = LSOA11CD, OA11CD or LSOA11NM
    search = search terms to find in the LSOA11NM column. Only needed if
        intending to dissolve on a name in the LSOA11NM column
    Returns: (gpd.DataFrame) agregated multipolygons, agregated on LSOA,
        OA code, or a search in the LSOA11NM column
    """
    if dissolveby in ['LSOA11CD', 'OA11CD']:
        polygon_df = geo_df.dissolve(by=dissolveby)
    else:
        filtered_df = geo_df[geo_df[f'{dissolveby}'].str.contains(search)]
        filtered_df.insert(0, 'place_name', search)
        polygon_df = filtered_df.dissolve(by='place_name')
    polygon_df = gpd.GeoDataFrame(polygon_df.pop('geometry'))
    return polygon_df


just_birmingham_poly = (get_polygons_of_loccode(
    geo_df=uk_LSOA_df, dissolveby='LSOA11NM', search="Birmingham"))


# Creating Birmingham polygon

# just_birmingham_geom = just_birmingham_LSOA.drop(["FID","LSOA11CD","LSOA11NMW","Age_Indica", "Shape__Are","Shape__Len","LSOA11NM"], axis=1)
# # just_birmingham_geom = just_birmingham_LSOA.pop('geometry')

# just_birmingham_geom['city'] = "birmingham"
# # just_birmingham_geom['new_column'] = 0
# just_birmingham_geom = just_birmingham_geom.dissolve(by='city')
# just_birmingham_geom


# Creating a Geo Dataframe of only stops in Birmingham
# import ipdb; ipdb.set_trace()
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

# Joining the population dataframe to the centroids dataframe
Wmids_pop_df = Wmids_pop_df.join(
    other=uk_pop_wtd_centr_df.set_index('OA11CD'), on='OA11CD', how='left')

# Make B'ham LSOA
bham_LSOA_df = uk_LSOA_df[uk_LSOA_df.LSOA11NM.str.contains("Birmingham")]
bham_LSOA_df = bham_LSOA_df[['LSOA11CD', 'LSOA11NM','geometry']]

# merge the two dataframes limiting to just Birmingham
bham_pop_df = Wmids_pop_df.merge(bham_LSOA_df,
                                 how='right',
                                 left_on='LSOA11CD',
                                 right_on='LSOA11CD',
                                 suffixes=('_pop', '_LSOA'))
# rename the All Ages column
bham_pop_df.rename(columns={"All Ages":"pop_count"}, inplace=True)
# change pop_count to number (int)
bham_pop_df['pop_count'] = pd.to_numeric(bham_pop_df.pop_count.str.replace(",", ""))

# create a buffer around the stops, in column "geometry"
# the `buffer_points` function changes the df in situ
_ = buffer_points(birmingham_stops_geo_df)

# grab some coordinates for a little section of Birmingham: Acocks Green
ag_east_north = pd.read_csv('Acocks_Green_postcodes.csv', usecols=['Easting', 'Northing'])
eastings = [easting for easting in ag_east_north.Easting]
northings = [northing for northing in ag_east_north.Northing]
east_min = min(eastings)
east_max = max(eastings)
north_min = min(northings)
north_max = max(northings)
# limit the stops, filtering by the min/max eastings/northings for Acocks Green
north_mask = (north_min < birmingham_stops_geo_df['Northing']) & (birmingham_stops_geo_df['Northing'] < north_max)
east_mask = (east_min < birmingham_stops_geo_df['Easting']) & (birmingham_stops_geo_df['Easting'] < east_max)
# Filter the stops for Acocks Green
ag_stops_geo_df = birmingham_stops_geo_df[north_mask & east_mask]

# Create the polygon for the combined buffered stops in Acocks Green
ag_stops_poly = unary_union(list(ag_stops_geo_df.geometry))

# Create the polygon for the combined buffered stops in B'ham
bham_stops_poly = unary_union(list(birmingham_stops_geo_df.geometry))

# Plot all the buffered stops in B'ham and AG on to a map
fig, ax = plt.subplots()
p = gpd.GeoSeries(bham_stops_poly)
# Plot Acocks Green only stops
a = gpd.GeoSeries(ag_stops_poly)
p.plot(ax=ax)
a.plot(ax=ax, color='gold')
plt.show()
fig.savefig('bham_ag_stops.png') 