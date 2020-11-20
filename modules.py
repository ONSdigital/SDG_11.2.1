import pandas as pd
import geopandas as gpd
from shapely.geometry import Point, Polygon
import requests
import os
import json

def geo_df_from_csv(path_to_csv, geom_x, geom_y, delim='\t', crs ='epsg:27700'):
    """Function to create a Geo-dataframe from a csv file.
        The process goes via Pandas
    
        Arguments:
            path_to_csv (string): path to the txt/csv containing geo data
                to be read
            delimiter (string): the seperator in the csv file e.g. "," or "\t" 
            geom_x (string):name of the column that contains the longitude data
            geom_y (string):name of the column that contains the latitude data
            crs (string): the coordinate reference system required
        Returns:
            Geopandas Dataframe
            """
    pd_df = pd.read_csv(path_to_csv, delim)
    geometry = [Point(xy) for xy in zip(pd_df[geom_x], pd_df[geom_y])]
    geo_df = gpd.GeoDataFrame(pd_df, geometry=geometry)
    geo_df.crs = crs
    return geo_df

def geo_df_from_geospatialfile(path_to_file, crs='epsg:27700'):
    
    """Function to create a Geo-dataframe from a geospatial (geojson, shp) file.
        The process goes via Pandas
    
        Arguments:
            path_to_file (string): path to the geojson, shp and other geospatial data files

        Returns:
            Geopandas Dataframe
            """
    geo_df = gpd.read_file(path_to_file)
    if geo_df.crs != crs:
        geo_df = geo_df.to_crs('epsg:27700')
    return geo_df

def find_points_in_poly(geo_df, polygon_obj):
    """Find points in polygon using geopandas' spatial join.
        Then drops all rows where the point is not in the polygon
        (based on column index_right not being NaN). Finally it
        drop all column names from that were created in the join,
        leaving only the columns of the original geo_df
        
        Arguments:
            geo_df (string): name of a geo pandas dataframe
            polygon_obj (string): a geopandas dataframe with a polygon column
            
        Returns:
            A geodata frame with the points inside the supplied polygon"""
    wanted_cols = geo_df.columns.to_list()
    joined_df = (gpd.sjoin
                 (geo_df,
                  polygon_obj,
                  how='left',
                  op='intersects')) #op = 'within'
    filtered_df = (joined_df
                   [joined_df
                    ['index_right'].notna()])
    filtered_df = filtered_df[wanted_cols]
    return filtered_df

def get_and_save_geo_dataset(url, localpath, filename):
    """Fetches a geodataset in json format from a web resource and 
        saves it to the local data/ directory and returns the json 
        as a dict into memory
    
    Args:
        filename (string): the name of file as it should be saved locally
        url (string): URL of the web resource where json file is hosted
        localpath (string): path to folder where json is to be saved locally
    Returns:
        json data as dict"""
    file = requests.get(url).json()
    full_path = os.path.join(localpath, filename)
    with open(full_path, 'w') as dset:
        json.dump(file, dset)
    return file

def draw_5km_buffer(centroid):
    """
    Draws a 5km (radius) buffer around a point. As 'epsg:27700' projections units of km, 
        500m is 0.5km.
    """
    distance_km = 0.5
return centroid.buffer(distance=distance_km)

