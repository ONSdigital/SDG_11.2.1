# Core imports for this module
import os
import json
from functools import lru_cache

# Third party imports for this module
import geopandas as gpd
import pandas as pd
import requests
from shapely.geometry import Point
from zipfile import ZipFile
import pyarrow.feather as feather
from typing import List, Set, Dict, Tuple, Optional, Union

# Types
PathLike = Union[str, bytes, os.PathLike]

def any_to_pd(file_nm: str, zip_link: str, ext_order: List, dtypes:Optional[Dict]) -> pd.DataFrame:
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
    load_order = [f"{file_nm}.{ext}" for ext in ext_order]
    # make a list of functions that apply to these files
    load_funcs = {"feather":feath_to_df,
                  "csv":csv_to_df,
                  "zip":import_extract_delete_zip}
    # create a dictionary ready to dispatch functions
    # load_func_dict = {f"{file_name}":load_func for file_name, load_func in zip(load_order, load_funcs)}
    # args_dict = 
    # Iterate through files that might exist
    for i in range(len(load_order)):
        data_file_nm = load_order[i] # Indexing because the list was loading in the wrong order
        data_file_path = make_data_path("data", data_file_nm)
        if persistent_exists(data_file_path): # Check if each persistent file exists
            # load the persistent file by dispatching the correct function
            if dtypes and ext_order[i]=="csv":
                pd_df = load_funcs[ext_order[i]](file_nm, data_file_path, dtypes=dtypes)
            else:
                pd_df = load_funcs[ext_order[i]](file_nm, data_file_path)
            return pd_df
        continue # None of the persistent files has been found. 
    # Continue onto the next file type 
    # A zip must be downloaded, extracted, and turned into pd_df
    pd_df = load_func_dict[data_file_nm](file_nm, 
                                        data_file_path,
                                        persistent_exists=False,
                                        zip_url=zip_link,
                                        dtypes=dtypes)
    return pd_df

def feath_to_df(file_nm, feather_path: PathLike):
    print(f"Reading {file_nm}.feather from {feather_path}.")
    pd_df = pd.read_feather(feather_path)
    return pd_df

def csv_to_df(file_nm, csv_path: PathLike, dtypes: Optional[Dict]): #*cols: Optional[List],
    print(f"Reading {file_nm}.csv from {csv_path}.")
    if dtypes:
        cols = list(dtypes.keys())
        pd_df = pd.read_csv(csv_path, usecols=cols, dtype=dtypes)
    else:
        pd_df = pd.read_csv(csv_path)
    pd_to_feather(pd_df, csv_path)
    return pd_df
    
def import_extract_delete_zip(file_nm: str, zip_path:PathLike, 
                              persistent_exists=True, 
                              zip_url=None,
                              *cols,
                              **dtypes):
    if not persistent_exists:
        grab_zip(file_nm, zip_url, zip_path)
    
    csv_nm = file_nm + ".csv"
    csv_path = make_data_path("data", csv_nm)
    extract_zip(file_nm, csv_nm, zip_path, csv_path)
    delete_junk(file_nm, zip_path)
    pd_df = csv_to_df(file_nm, csv_path, cols, dtypes)
    return pd_df

def grab_zip(file_nm: str, zip_link, zip_path: PathLike):
    # Grab the zipfile from gov.uk
    print(f"Dowloading {file_nm} from {zip_link}")
    r = requests.get(zip_link)
    with open(zip_path, 'wb') as output_file:
        print(f"Saving {file_nm} to {zip_path}")
        output_file.write(r.content)

def extract_zip(file_nm: str, csv_nm, zip_path, csv_path):
    # Open the zip file and extract
    with ZipFile(zip_path, 'r') as zip:
        print(f"Extracting {csv_nm} from {zip_path}")
        _ = zip.extract(csv_nm, "data")

def delete_junk(file_nm: str, zip_path):
    # Delete the zipfile as it's uneeded now
    print(f"Deleting {file_nm} from {zip_path}")
    os.remove(zip_path)

@lru_cache
def make_data_path(*data_dir_files: str) -> PathLike:
    """Makes a relative path pointing to the data directory

    Args:
        data_dir_files (str): folder name(s) (e.g. name(s) of the 
            data directory) and name of the file to build a path to

    Returns:
        PathLike: a combination of the data directory and the 
            filename, suitable for the operating system.
    """    
    data_path = os.path.join(*data_dir_files)
    return data_path

@lru_cache 
def persistent_exists(persistent_file_path):
    """Checks if a persistent file already exists or not. 
        Since persistent files will be Apache feather format 
        currently the function just checks for those"""
    if os.path.isfile(persistent_file_path):
        print(f"{persistent_file_path} already exists")
        return True
    else:
        print(f"{persistent_file_path} does not exist")
        return False 

def pd_to_feather(pd_df, current_file_path):
    """Writes a Pandas dataframe to feather for quick reading 
        and retrieval later.
    """
    feather_path = os.path.splitext(current_file_path)[0]+'.feather'
    if not os.path.isfile(feather_path):
        print(f"Writing Pandas dataframe to feather at {feather_path}")
        feather.write_feather(pd_df, feather_path)
        return True
    print("Feather already exists")

def geo_df_from_pd_df(pd_df, geom_x, geom_y, crs):
    """Function to create a Geo-dataframe from a Pandas DataFrame.

        Arguments:
            pd_df (pd.DataFrame): a pandas dataframe object to be converted
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
