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

def dl_csv_make_df(data_file_nm, zip_link, data_dir):
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
    # Make full path
    data_file_path = make_data_path(data_dir, data_file_nm)
    load_order = [f"{data_file_nm}.{ext}" for ext in ['feather','csv','zip']]
    load_funcs = [feather_to_pd, csv_to_df, import_extract_delete_zip]
    load_func_dict = {f"{data_file_nm}":f"{load_func}" for file_name, load_func in load_order, load_funcs}
    # Iterate through files that might exist
    for data_file_nm in load_order:
        data_file_path = make_data_path(data_dir, data_file_nm)
        if persistent_exists(data_file_path): # Check if each persistent file exists
            # load the persistent file
            pd_df = load_func_dict[data_file_nm](data_file_path)
            return pd_df
        continue # continue onto the next file type
    load_func_dict[data_file_nm](data_file_path, downloaded_file=False)


    return True

def csv_to_df(csv_path):
    return pd.read_csv()

def import_extract_delete_zip(zip_path, downloaded_file=True):
    if not downloaded_file:
        grab_zip(zip_path)
    extract_zip(zip_path)
    delete_junk(zip_path)
    csv_to_df(zip_path)

def grab_zip(zip_link, zip_path):
    # Grab the zipfile from gov.uk
    print(f"Dowloading file from {zip_link}")
    r = requests.get(zip_link)
    with open(zip_path, 'wb') as output_file:
        print(f"Saving to {zip_path}")
        output_file.write(r.content)

def extract_zip(zip_name, csv_nm):
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
        print(f"{file_type} already exists")
        return True
    else:
        print(f"{file_type} does not exist")
        return False 

def pd_to_feather(pd_df, feather_path):
    """Writes a Pandas dataframe to feather for quick reading 
        and retrieval later.
    """
    print(f"Writing Pandas dataframe to feather at {feather_path}")
    feather.write_feather(pd_df, feather_path)
    return True

def feather_to_pd(feather_path):
    """
    Imports data from a an Apache feather file 
        and returns a datafame into memory

    Args:
        feather_path (string): the path to the feather file

    Returns:
        pd.DataFrame: pandas dataframe of the data  
    """    
    print("Converting feather to Pandas")
    pd_df = pd.read_feather(feather_path)
    return pd_df

def geo_df_from_csv(path_to_csv, geom_x, geom_y, cols, crs, delim=','):
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
