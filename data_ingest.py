# Core imports for this module
import os
import json

# Third party imports for this module
import geopandas as gpd
import pandas as pd
import requests
from shapely.geometry import Point
from zipfile import ZipFile
import pyarrow.feather as feather

def any_to_pd(data_file_nm, zip_link):
    """
    A function which ties together many other data ingest related functions.
        The main purpose is to check for locally stored persistent data 
        files and get that data into a dataframe.
        Each time checking for download/extracted data so it doesn't have to
        go through the process again.
        Firstly it checks for a feather file and loads that if available.
        If the feather is not available the scipt may not have been run 
        before, so it checks for a csv file, then loads that if available. 
        If no csv file is available it checks for a zip file, from which to 
        extract the csv from. If the zip is not available locally it falls back
        downloading the zip file (which contains quite a few un-needed datasets)
        then extracts the needed data set (csv) and eletes the now un-needed 
        zip file.
        
    """

    # Make the load order (lists are ordered) to prioritise
    load_order = [f"{data_file_nm}.{ext}" for ext in ['feather','csv','zip']]
    # make a list of functions that apply to these files
    load_funcs = [pd.read_feather, pd.read_csv, import_extract_delete_zip]
    # create a dictionary ready to dispatch functions
    load_func_dict = {f"{file_name}":load_func for file_name, load_func in zip(load_order, load_funcs)}
    args_dict = 
    # Iterate through files that might exist
    for data_file_nm in load_order:
        data_file_path = make_data_path("data", data_file_nm)
        if persistent_exists(data_file_path): # Check if each persistent file exists
            # load the persistent file by dispatching the correct function
            pd_df = load_func_dict[data_file_nm](data_file_path)
            pd_to_feather(pd_df, data_file_path)
            return pd_df
        continue # Persistent not found. Continue onto the next file type
    # None of the persistent files has been found. 
    # A zip must be downloaded, extracted, and turned into pd_df
    pd_df = load_func_dict[data_file_nm](data_file_path,
                                        persistent_exists=False,
                                        zip_url=zip_link)
    return pd_df

def import_extract_delete_zip(zip_path, persistent_exists=True, zip_url=None, **kwargs):
    if not persistent_exists:
        print(f"Downloading zip from {zip_url}")
        grab_zip(zip_url, zip_path)
    print("Extracting csv from zip returning pd_df")
    extract_zip(zip_path)
    delete_junk(zip_path)
    pd_df = pd.read_csv(zip_path)
    return pd_df

def grab_zip(zip_link, zip_path):
    # Grab the zipfile from gov.uk
    print(f"Dowloading file from {zip_link}")
    r = requests.get(zip_link)
    with open(zip_path, 'wb') as output_file:
        print(f"Saving to {zip_path}")
        output_file.write(r.content)

def extract_zip(zip_name):
    # Open the zip file and extract
    with ZipFile(zip_path, 'r') as zip:
        print(f"Unzipping {zip_name}. Extracting {csv_nm}")
        _ = zip.extract(csv_nm, path=data_dir)

def delete_junk(zip_name, zip_path):
    # Delete the zipfile as it's uneeded now
    print(f"Deleting {zip_name} from {zip_path}")
    os.remove(zip_path)

def make_data_path(data_dir, data_file_nm):
    data_path = os.path.join(data_dir, data_file_nm)
    return data_path

def get_file_ext(persistent_file_path):
    """Get file extension. Returns a tuple of root[0] and extension[1]"""
    file_ext =  os.path.splitext(persistent_file_path)[1]
    return file_ext

def persistent_exists(persistent_file_path):
    """Checks if a persistent file already exists or not. 
        Since persistent files will be Apache feather format 
        currently the function just checks for those"""
    file_type = get_file_ext(persistent_file_path)
    if os.path.isfile(persistent_file_path):
        print(f"{file_type[1:]} already exists")
        return True
    else:
        print(f"{file_type[1:]} does not exist")
        return False 

def pd_to_feather(pd_df, feather_path):
    """Writes a Pandas dataframe to feather for quick reading 
        and retrieval later.
    """
    if not os.path.isfile(feather_path):
        print(f"Writing Pandas dataframe to feather at {feather_path}")
        feather.write_feather(pd_df, feather_path)
        return True
    print("Feather already exists")

def geo_df_from_csv(pd_df, geom_x, geom_y, cols, crs, delim=','):
    """Function to create a Geo-dataframe from a csv file.
        The process goes via Pandas

        Arguments:
            pd_df (pd.DataFrame): a pandas dataframe object to be converted
            delimiter (string): the seperator in the csv file e.g. "," or "\t"
            geom_x (string):name of the column that contains the longitude data
            geom_y (string):name of the column that contains the latitude data
            crs (string): the coordinate reference system required
        Returns:
            Geopandas Dataframe
            """

    geometry = [Point(xy) for xy in zip(pd_df[geom_x], pd_df[geom_y])]
    geo_df = gpd.GeoDataFrame(pd_df, geometry=geometry)
    geo_df.crs = crs
    geo_df.to_crs(crs, inplace=True)
    return geo_df


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
