import pandas as pd
import geopandas as gpd
from shapely.geometry import Point
import requests
import os
import json
from zipfile import ZipFile


def geo_df_from_csv(path_to_csv, geom_x, geom_y, cols, delim=',', crs='EPSG:27700'):
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
    # possibly useful code nrows=nrows for getting a sample of csv
    pd_df = pd.read_csv(path_to_csv,
                        delim,
                        engine="python",
                        error_bad_lines=False,
                        quotechar='"',
                        usecols=cols)
    geometry = [Point(xy) for xy in zip(pd_df[geom_x], pd_df[geom_y])]
    geo_df = gpd.GeoDataFrame(pd_df, geometry=geometry)
    geo_df.crs = crs
    geo_df.to_crs(crs, inplace=True)
    return geo_df


def geo_df_from_geospatialfile(path_to_file, crs='epsg:27700'):
    """Function to create a Geo-dataframe from a geospatial
        (geojson, shp) file. The process goes via Pandas.s

        Arguments:
            path_to_file (string): path to the geojson, shp and other
                geospatial data files

        Returns:
            Geopandas Dataframe
            """
    geo_df = gpd.read_file(path_to_file)
    if geo_df.crs != crs:
        geo_df = geo_df.to_crs('epsg:27700')
    return geo_df


def find_points_in_poly(geo_df, polygon_obj):
    """Find points in polygon using geopandas' spatial join
        which joins the supplied geo_df (as left_df) and the
        polygon (as right_df).

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
                  op='intersects'))  # op = 'within'
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
    Draws a 5km (radius) buffer around a point. As 'epsg:27700' projections
    units of km so 500m is 0.5km.
    """
    distance_km = 0.5
    return centroid.buffer(distance=distance_km)


def dl_csv_make_df(csv_nm, csv_path, zip_name, zip_path, zip_link, data_dir):
    """
    Downloads the zip file (which contains quite a few un-needed datasets)
    Extracts the needed data set (csv)
    Deletes the now un-needed zip file
    Checks if the csv is already download/extracted so it doesn't have to
    go through the process again.
    """
    # Check if csv exists
    if os.path.isfile(csv_path):
        print("csv already exists")
    else:
        # Check if zipfile exists
        if os.path.isfile(zip_path):
            print("Zip file already exists")
        else:
            # Grab the zipfile from gov.uk
            print(f"Dowloading file from {zip_link}")
            r = requests.get(zip_link)
            with open(zip_path, 'wb') as output_file:
                print(f"Saving to {zip_path}")
                output_file.write(r.content)
        # Open the zip file and extract
        with ZipFile(zip_path, 'r') as zip:
            print(f"Unzipping {zip_name}. Extracting {csv_nm}")
            _ = zip.extract(csv_nm, path=data_dir)
        # Delete the zipfile as it's uneeded now
        print(f"Delecting {zip_name} from {zip_path}")
        os.remove(zip_path)

    return True

def highly_serv_stops(region):
    """
    Retrieves timetable data from the Traveline National Dataset for
        any region. Filters stops with that have >1 departure per hour
        on a weekday (Wed is default) between 6am and 8pm.
    Parameters: 
        region (str): the name of the region of the UK that the data
            is needed for
    Returns:
        highly serviced stops for region    
    """
    day="Wed"
   
    return 


def demarc_urb_rural(urbDef, ):
    """
    Creates spatial clusters of urban environments based on specified
        definition of 'urban'. 
        - engwls for the English/Welsh definition of urban
        - scot for the Scottish definition of urban
        - eur for the European definition of urban
    
    Parameters:
        urbDef (str): the definition of urban to be used
    Returns: TBC (probably a polygon)
            """


def create_db_connection(host_name, user_name, user_password, db_name):
    """Connecting to SQL Server (probably MySQL eventually BigQuery)"""
    connection = None
    try:
        connection = mysql.connector.connect(
            host=host_name,
            user=user_name,
            passwd=user_password,
            database=db_name
        )
        print("MySQL Database connection successful")
    except Error as err:
        print(f"Error: '{err}'")

    return connection

def populate_table():
    """Populating the SQL Tables"""


def read_query(connection, query):
    """Reading Data formatting Output into a pandas DataFrame"""
    cursor = connection.cursor()
    result = None
    try:
        cursor.execute(query)
        result = cursor.fetchall()
        # Turn into df
        return result
    except Error as err:
        print(f"Error: '{err}'")


def buffer_points(geo_df, distance_km=500):
    """
    Provide a Geo Dataframe with points you want buffering.
    Draws a 5km (radius) buffer around the points.
    Puts the results into a new column called "buffered"
    As 'epsg:27700' projections units of km, 500m is 0.5km.
    """
    geo_df['geometry'] = geo_df.geometry.buffer(distance_km)
    return geo_df