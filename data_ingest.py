# Core imports for this module
import os
import re
import json
from functools import lru_cache, reduce
from time import perf_counter
from attr import resolve_types
import yaml

# Third party imports for this module
import geopandas as gpd
import pandas as pd
import requests
from shapely.geometry import Point
from zipfile import ZipFile
import pyarrow.feather as feather
from typing import List, Dict, Optional, Union

# Defining Custom Types
PathLike = Union[str, bytes, os.PathLike]

#Config
CWD = os.getcwd()
with open(os.path.join(CWD, "config.yaml")) as yamlfile:
    config = yaml.load(yamlfile, Loader=yaml.FullLoader)
    print("Config loaded")
DATA_DIR = config["DATA_DIR"]


def any_to_pd(file_nm: str,
              zip_link: str,
              ext_order: List,
              dtypes: Optional[Dict]) -> pd.DataFrame:
    """
    A function which ties together many other data ingest related functions to 
        to import data. 

        Currently this function can handle the remote or local import of data 
        from zip (containing csv files), and csv files.
        
        The main purpose is to check for locally stored persistent data
        files and get that data into a dataframe for further processing.

        Each time the function checks for download/extracted data so it 
        doesn't have to go through the process again.

        Firstly the function checks for a feather file and loads that if 
        available.

        If the feather is not available the function checks for a csv file,
        then loads that if available.

        If no csv file is available it checks for a zip file, from which to
        extract the csv from.
        
        If a zip file is not available locally it falls back to downloading
        the zip file (which could contains other un-needed datasets)
        then extracts the specified/needed data set (csv) and deletes the
        now un-needed zip file.

        This function should be used in place of pd.read_csv for example.

    TODO: extend this function to handle API import of json data - may already be done in get_and_save_geo_dataset function
    
    Returns:
        pd.DataFrame: A dataframe of the data that has been imported
    """

    # Make the load order (lists are ordered) to prioritise
    load_order = [f"{file_nm}.{ext}" for ext in ext_order]
    # make a list of functions that apply to these files
    load_funcs = {"feather": _feath_to_df,
                  "csv": _csv_to_df,
                  "zip": _import_extract_delete_zip}
    # create a dictionary ready to dispatch functions
    # load_func_dict = {f"{file_name}": load_func

    # Iterate through files that might exist
    for i in range(len(load_order)):
        # Indexing with i because the list loading in the wrong order
        data_file_nm = load_order[i]
        data_file_path = _make_data_path("data", data_file_nm)
        if _persistent_exists(data_file_path):
            # Check if each persistent file exists
            # load the persistent file by dispatching the correct function
            if dtypes and ext_order[i] == "csv":
                pd_df = load_funcs[ext_order[i]](file_nm,
                                                 data_file_path,
                                                 dtypes=dtypes)
            else:
                pd_df = load_funcs[ext_order[i]](file_nm,
                                                 data_file_path)
            return pd_df
        continue  # None of the persistent files has been found.
    # Continue onto the next file type
    # A zip must be downloaded, extracted, and turned into pd_df
    pd_df = load_funcs[ext_order[i]](file_nm,
                                     data_file_path,
                                     persistent_exists=False,
                                     zip_url=zip_link,
                                     dtypes=dtypes)
    return pd_df


def _feath_to_df(file_nm: str, feather_path: PathLike) -> pd.DataFrame:
    """Feather reading function used by the any_to_pd
        fucntion.

    Args:
        file_nm (str): the name of the file without extension
        feather_path (PathLike): the path/to/the/featherfile

    Returns:
        pd.DataFrame: Pandas dataframe read from the persistent feather file
    """        
    print(f"Reading {file_nm}.feather from {feather_path}.")
    tic = perf_counter()
    pd_df = pd.read_feather(feather_path)
    toc = perf_counter()
    print(f"""Time taken for {file_nm}.feather 
          reading is {toc - tic:.2f} seconds""")
    return pd_df


def _csv_to_df(file_nm: str, csv_path: PathLike, dtypes: Optional[Dict], persistent_exists=None, zip_url=None) -> pd.DataFrame:
    
    print(f"Reading {file_nm}.csv from {csv_path}.")
    if dtypes:
        cols = list(dtypes.keys())
        tic = perf_counter()
        pd_df = pd.read_csv(csv_path, usecols=cols, dtype=dtypes, encoding_errors="ignore")
        toc = perf_counter()
        print(f"Time taken for csv reading is {toc - tic:.2f} seconds")
    else:
        pd_df = pd.read_csv(csv_path)
    # Calling the pd_to_feather function to make a persistent feather file
    # for faster retrieval
    _pd_to_feather(pd_df, csv_path)
    return pd_df


def _import_extract_delete_zip(file_nm: str, zip_path: PathLike,
                              persistent_exists=True,
                              zip_url=None,
                              *cols,
                              **dtypes) -> pd.DataFrame:
    if not persistent_exists:
        # checking if a persistent zip exists to save downloading
        _grab_zip(file_nm, zip_url, zip_path)

    csv_nm = file_nm + ".csv"
    csv_path = _make_data_path("data", csv_nm)
    _extract_zip(file_nm, csv_nm, zip_path, csv_path)
    _delete_junk(file_nm, zip_path)
    pd_df = _csv_to_df(file_nm, csv_path, dtypes)
    return pd_df


def _grab_zip(file_nm: str, zip_link, zip_path: PathLike):
    """Used by _import_extract_delete_zip function to download
        a zip file from the URI specified in the the zip_link 
        parameter.
        
       This function does not return anything but 

    Args:
        file_nm (str): the name of the file without extension
        zip_link (str): URI to the zip to be downloaded
        zip_path (PathLike): path/to/the/write/directory, e.g. '/data/'
    """    
    # Grab the zipfile from URI
    print(f"Dowloading {file_nm} from {zip_link}")
    r = requests.get(zip_link)
    with open(zip_path, 'wb') as output_file:
        print(f"Saving {file_nm} to {zip_path}")
        output_file.write(r.content)


def _extract_zip(file_nm: str, csv_nm: str, zip_path: PathLike, csv_path: PathLike):
    """Used by _import_extract_delete_zip function to extract
        the needed csv dataset from the locally stored zip file.

    Args:
        file_nm (str): the name of the file without extension
        csv_nm (str): the name of the csv file that is 
                      expected inside the zip file
        zip_path (PathLike): path/to/local/zip/file.zip
        csv_path (PathLike): The path where the csv should be written to, e.g. /data/
    """    
    # Open the zip file and extract
    # TODO: Correction: zip.extract should be writing to the csv_path, not to "data".
    with ZipFile(zip_path, 'r') as zip:
        print(f"Extracting {csv_nm} from {zip_path}")
        _ = zip.extract(csv_nm, "data")


def _delete_junk(file_nm: str, zip_path: PathLike):
    """Used by _import_extract_delete_zip function to delete the
        zip file after it was downloaded and the needed data extracted
        it.

    Args:
        file_nm (str): the name of the file without extension
        zip_path (PathLike): path/to/local/zip/file.zip that is to deleted
    """   
    # Delete the zipfile as it's uneeded now
    print(f"Deleting {file_nm} from {zip_path}")
    os.remove(zip_path)


@lru_cache
def _make_data_path(*data_dir_files: str) -> PathLike:
    """Makes a relative path pointing to the data directory.
    
    This was created to avoid repeated using
        os.path.join(somepath, somefile) all over the script.

    Args:
        data_dir_files (str): folder name(s) (e.g. name(s) of the
            data directory) and name of the file to build a path to

    Returns:
        PathLike: a combination of the data directory and the
            filename, suitable for the operating system. Note: PathLike
            has been defined as a custom data type in this script.
    """
    data_path = os.path.join(*data_dir_files)
    return data_path


@lru_cache
def _persistent_exists(persistent_file_path):
    """Checks if a persistent file already exists or not.
        Since persistent files will be Apache feather format
        currently the function just checks for those"""
    if os.path.isfile(persistent_file_path):
        print(f"{persistent_file_path} already exists")
        return True
    else:
        print(f"{persistent_file_path} does not exist")
        return False


def _pd_to_feather(pd_df: pd.DataFrame, current_file_path: PathLike):
    """Used by the any_to_pd function to writes a Pandas dataframe
        to feather for quick reading and retrieval later.

        This function returns nothing because it simply writes to disk.
    """    
    feather_path = os.path.splitext(current_file_path)[0]+'.feather'
    
    # TODO: this function should make use of _persistent_existsD    
    if not os.path.isfile(feather_path):
        print(f"Writing Pandas dataframe to feather at {feather_path}")
        feather.write_feather(pd_df, feather_path)
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


def capture_region(file_nm: str):
    "Extracts the region name from the ONS population estimate excel files."
    patt = re.compile("^(.*estimates-)(?P<region>.*)(\.xls)")
    region = re.search(patt, file_nm).group("region")
    region = region.replace("-", " ").capitalize()
    return region

def get_whole_nation_pop_df(pop_files, pop_year):
    """Gets the population data for all regions in the country and puts them into one dataframe

    Returns:
        pd.DataFrame: Dataframe of population data for all regions in the country
    """
    # Dict of region:file_name. Capture the region name from the filename
    region_dict = {capture_region(file):file for file in pop_files}
    # make a df of each region then concat
    national_pop_feather_path = os.path.join(DATA_DIR, "whole_nation.feather")
    if not os.path.exists(national_pop_feather_path):
        print("No national_pop_feather found")
        region_dfs_dict = {}
        for region in region_dict:
            print(f"Reading {region} Excel file")
            xls_path = os.path.join(
                                DATA_DIR,
                                "population_estimates",
                                str(pop_year),
                                region_dict[region])
        # Read Excel file as object
            xlFile = pd.ExcelFile(xls_path)
        # Access sheets in Excel file
            total_pop = pd.read_excel(xlFile, "Mid-2019 Persons", header=4)
            males_pop = pd.read_excel(xlFile, "Mid-2019 Males", header=4, usecols=["OA11CD", "LSOA11CD", "All Ages"])
            fem_pop = pd.read_excel(xlFile, "Mid-2019 Females", header=4, usecols=["OA11CD", "LSOA11CD", "All Ages"])
        # Rename the "All Ages" columns appropriately before concating
            total_pop.rename(columns={"All Ages": "pop_count"}, inplace=True)
            males_pop.rename(columns={"All Ages": "males_pop"}, inplace=True)
            fem_pop.rename(columns={"All Ages": "fem_pop"}, inplace=True)
        # Merge the data from different sheets
            dfs_to_merge = [total_pop, males_pop, fem_pop]
            df_final = reduce(lambda left, right: pd.merge(left, right, on='OA11CD'), dfs_to_merge)
        # Store merged df in dict under region name
            region_dfs_dict[region] = df_final
    # Concat all regions into national pop df
        whole_nation_pop_df = pd.concat(region_dfs_dict.values())
    # Create the pop_year column to show which year the data is from
        whole_nation_pop_df["pop_year"] = pop_year
    # Change all column names to str
        whole_nation_pop_df.columns = whole_nation_pop_df.columns.astype(str)
    # Write df out to feather for quicker reading
        print("Writing whole_nation_pop_df.feather")
        whole_nation_pop_df.reset_index().to_feather(national_pop_feather_path)
    else:
    # if it exists, read from a feather for quicker data retreval
        print(f"Reading whole_nation_pop_df from {national_pop_feather_path}")
        whole_nation_pop_df = pd.read_feather(national_pop_feather_path)
    # Temporary TODO: remove this line
        whole_nation_pop_df.rename(columns={"total_pop": "pop_count"}, inplace=True)
    return whole_nation_pop_df
