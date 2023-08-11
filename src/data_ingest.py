# Core imports for this module
import os
import re
import json
from functools import lru_cache, reduce
from time import perf_counter
import yaml
from datetime import datetime, timedelta
import logging
import glob

# Google Cloud imports for this module
from google.oauth2 import service_account
from google.auth.transport.requests import Request
from google.cloud import storage

# Third party imports for this module
import geopandas as gpd
import pandas as pd
import requests
from zipfile import ZipFile
import pyarrow.feather as feather
from typing import List, Dict, Optional, Union
import numpy as np

# Defining Custom Types
PathLike = Union[str, bytes, os.PathLike]
MainLogger = logging.getLogger(__name__)

# Config
CWD = os.getcwd()
with open(os.path.join(CWD, "config.yaml")) as yamlfile:
    config = yaml.load(yamlfile, Loader=yaml.FullLoader)
    module = os.path.basename(__file__)
    print(f"Config loaded in {module}")
DATA_DIR = config["data_dir"]
CLOUD_LOCAL = config["cloud_local"]

class GCPBucket:
    """
    This class sets up an instance of a GCP storage client with the right credentials.

    The methods either download a file from the bucket or generate a signed url for a file.
    """
    def __init__(self):
        self.credentials = service_account.Credentials.from_service_account_file(glob.glob('secrets/*.json')[0])
        self.client = storage.Client(credentials=self.credentials)
        self.bucket_name = '11-2-1-all-data'
        self.bucket = self.client.bucket(self.bucket_name)


    def download_file(self, file_name, destination_file_name):
        """Downloads a blob from the bucket."""

        blob = self.bucket.blob(file_name)

        blob.download_to_filename(destination_file_name)

        print(
            f"Blob {self.client} downloaded to {destination_file_name}."
            )

    def generate_signed_url(self, object_name):
        """ This will generate a signed URL that is valid for 5 minutes.

        The URL should be able to be used by our data ingest functions
            to download any file."""
        expiration = datetime.utcnow() + timedelta(minutes=5)
        url = self.bucket.blob(object_name).generate_signed_url(
            expiration,
            method='GET',
            credentials=self.credentials,
        )
        return url

    def get_file_list(self):
        """This will return a list of all the files in the bucket, as "blobs"."""

        files = self.bucket.list_blobs()

        return files

    def get_file_names(self):
        """Prints and returns a list of all the file names in the bucket.

        Returns:
            List: List of file names in the bucket.
        """

        files = self.bucket.list_blobs()
        file_names = [file.name for file in files]

        print(f"Found {len(file_names)} files in the bucket.")

        for file in file_names:
            print(file)

        return file_names

bucket = GCPBucket()

def path_or_url(file_path):
    if CLOUD_LOCAL == "cloud":
        url = bucket.generate_signed_url(file_path)
        return url
    elif CLOUD_LOCAL == "local":
        return file_path
    else:
        MainLogger.error("Cloud or local configuration is incorrect")
        raise ImportError

def feath_to_df(file_nm: str, feather_path: PathLike) -> pd.DataFrame:
    """Feather reading function used by the any_to_pd function.

    Args:
        file_nm (str): the name of the file without extension.
        feather_path (PathLike): the path/to/the/featherfile.

    Returns:
        pd.DataFrame: Pandas dataframe read from the persistent feather file.
    """
    print(f"Reading {file_nm}.feather from {feather_path}.")
    # check if supplied path is a directory or a file
    if os.path.isdir(feather_path):
        # if directory then append the file name
        feather_path = os.path.join(feather_path, f"{file_nm}.feather")
    # Time the read
    tic = perf_counter()
    pd_df = pd.read_feather(path_or_url(feather_path))
    toc = perf_counter()
    print(f"""Time taken for {file_nm}.feather
          reading is {toc - tic:.2f} seconds""")
    return pd_df


def csv_to_df(
        file_nm: str,
        csv_path: PathLike,
        dtypes: Optional[Dict],
        persistent_exists=True,
        zip_url=None) -> pd.DataFrame:
    """Creates pandas DataFrame from csv; optionally using datatypes &
    selected columns.

    Sub func of both any_to_pd and _import_extract_delete_zip.

    Args:
        file_nm (str): The name of the source csv without the extension.
            e.g. "stops", not "stops.csv".
        csv_path (PathLike): The path/to/csv_file on local machine.
        dtypes (Optional[Dict]): Datatypes of columns in the csv.
            Helps optimise import.
        persistent_exists (bool, optional): Boolean supplied by the
            _persistent_exists function. Defaults to True.
        zip_url (str, optional): URL for the zip resource if it is to be
            downloaded. Defaults to None.

    Returns:
        pd.DataFrame
    """
    print(f"Reading {file_nm}.csv from {csv_path}.")
    if dtypes:
        cols = list(dtypes.keys())
        tic = perf_counter()
        pd_df = pd.read_csv(path_or_url(csv_path), usecols=cols,
                            dtype=dtypes, encoding_errors="ignore")
        toc = perf_counter()
        print(f"Time taken for csv reading is {toc - tic:.2f} seconds")
    else:
        pd_df = pd.read_csv(path_or_url(csv_path))
    # Calling the pd_to_feather function to make a persistent feather file
    # for faster retrieval
    pd_to_feather(pd_df, csv_path)
    return pd_df


def import_extract_delete_zip(file_nm: str, zip_path: PathLike,
                               persistent_exists=True,
                               zip_url=None,
                               *cols,
                               **dtypes) -> pd.DataFrame:
    """Downloads and opens zip file, extracts contents, deletes zip.

    Subfunc of any_to_pd.

    Args:
        file_nm (str): The name of the target csv within the zip.
        zip_path (PathLike): the path/to/zip_file on local machine
        persistent_exists (bool, optional): Boolean supplied by the
            _persistent_exists function. Defaults to True.
        zip_url (str, optional): URL for the zip resource if it is to be
            downloaded. Defaults to None.

    Returns:
        pd.DataFrame: dataframe of the data from the CSV
    """
    if not persistent_exists:
        # checking if a persistent zip exists to save downloading
        grab_zip(file_nm, zip_url, zip_path)

    csv_nm = file_nm + ".csv"
    csv_path = _make_data_path("data", csv_nm)
    extract_zip(file_nm, csv_nm, zip_path, csv_path)
    delete_junk(file_nm, zip_path)
    pd_df = csv_to_df(file_nm, csv_path, dtypes)
    return pd_df


def grab_zip(file_nm: str, zip_link, zip_path: PathLike):
    """Used by _import_extract_delete_zip function to download
    a zip file from the URI specified in the the zip_link
    parameter.

    Args:
        file_nm (str): the name of the file without extension.
        zip_link (str): URI to the zip to be downloaded.
        zip_path (PathLike): path/to/the/write/directory, e.g. '/data/'.
    """
    # Grab the zipfile from URI
    print(f"Downloading {file_nm} from {zip_link}")
    r = requests.get(zip_link)
    with open(zip_path, 'wb') as output_file:
        print(f"Saving {file_nm} to {zip_path}")
        output_file.write(r.content)


def extract_zip(
        file_nm: str,
        csv_nm: str,
        zip_path: PathLike,
        csv_path: PathLike):
    """Used by _import_extract_delete_zip function to extract
    the needed csv dataset from the locally stored zip file.

    Args:
        file_nm (str): the name of the file without extension.
        csv_nm (str): the name of the csv file that is
            expected inside the zip file.
        zip_path (PathLike): path/to/local/zip/file.zip.
        csv_path (PathLike): The path where the csv should be
            written to, e.g. /data/.
    """
    # Open the zip file and extract
    with ZipFile(zip_path, 'r') as zip:
        print(f"Extracting {csv_nm} from {zip_path}")
        try:
            _ = zip.extract(csv_nm, csv_path)
        except BaseException:
            csv_nm


def delete_junk(file_nm: str, zip_path: PathLike):
    """Used by _import_extract_delete_zip function to delete the
    zip file after it was downloaded and the needed data extracted
    it.

    Args:
        file_nm (str): the name of the file without extension.
        zip_path (PathLike): path/to/local/zip/file.zip that is to deleted.
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
def persistent_exists(persistent_path):
    """Checks if a persistent file or directory already exists or not.

    Args:
        persistent_path (PathLike): path to check.

    Returns:
        bool: True if a persistent file or directory already exists.
    """
    # Change directory into project root
    os.chdir(CWD)

    if os.path.exists(persistent_path):
        print(f"{persistent_path} already exists")
        return True
    else:
        print(f"{persistent_path} does not exist")
        return False


def pd_to_feather(pd_df: pd.DataFrame, current_file_path: PathLike):
    """Used by the any_to_pd function to writes a Pandas dataframe
    to feather for quick reading and retrieval later.

    This function returns nothing because it simply writes to disk.

    Args:
        pd_df (pd.DataFrame): a pandas dataframe to be converted
        current_file_path (PathLike): The path/to/current_file
            on local machine.
    """
    feather_path = os.path.splitext(current_file_path)[0] + '.feather'

    # TODO: this function should make use of _persistent_existsD
    if not os.path.isfile(feather_path):
        print(f"Writing Pandas dataframe to feather at {feather_path}")
        feather.write_feather(pd_df, feather_path)
    print("Feather already exists")


def get_and_save_geo_dataset(url, localpath, filename):
    """Fetches a geodataset in json format from a web resource and
    saves it to the local data/ directory and returns the json
    as a dict into memory.

    Args:
        filename (str): the name of file as it should be saved locally.
        url (str): URL of the web resource where json file is hosted.
        localpath (str): path to folder where json is to be saved locally.

    Returns:
        dict
    """
    file = requests.get(url).json()
    full_path = os.path.join(localpath, filename)
    with open(full_path, 'w') as dset:
        json.dump(file, dset)
    return file


def geo_df_from_geospatialfile(path_to_file, crs='epsg:27700'):
    """Function to create a Geo-dataframe from a geospatial
    (geojson, shp) file. The process goes via Pandas.s

    Args:
        path_to_file (str): path to the geojson, shp and other
            geospatial data files.

    Returns:
        Geopandas Dataframe
    """
    geo_df = gpd.read_file(path_to_file)
    if geo_df.crs is None:
        geo_df.crs = 'epsg:27700'
    elif geo_df.crs != crs:
        geo_df = geo_df.to_crs('epsg:27700')
    return geo_df


def capture_region(file_nm: str):
    """Extracts the region name from the ONS population estimate excel files.

    Args:
        file_nm (str): Full name of the regional population Excel file.

    Returns:
        str: The name of the region that the file covers
    """
    patt = re.compile("^(.*estimates[-]?)(?P<region>.*)(\.xls)")
    region = re.search(patt, file_nm).group("region")
    region = region.replace("-", " ").capitalize()
    return region


def get_whole_nation_pop_df(pop_files, pop_year):
    """Gets the population data for all regions in the country and
    puts them into one dataframe.

    Args:
        pop_files (list): Population data to be unioned.
        pop_year (str): The year of population estimation data to process.

    Returns:
        pd.DataFrame: Dataframe of population data for all regions
            in the country
    """
    # Remove gitkeep file from list of pop files
    pop_files = [f for f in pop_files if f != '.gitkeep']
    # Dict of region:file_name. Capture the region name from the filename
    region_dict = {capture_region(file): file for file in pop_files}
    # make a df of each region then concat
    national_pop_feather_path = os.path.join(
        DATA_DIR, f"whole_nation_{pop_year}.feather")
    if not os.path.exists(national_pop_feather_path):
        print(f"No national_pop_feather found for {pop_year}")
        print(f"Rebuilding population file. This will take a while!")
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
            total_pop = pd.read_excel(
                xlFile, f"Mid-{pop_year} Persons", header=4)
            males_pop = pd.read_excel(
                xlFile,
                f"Mid-{pop_year} Males",
                header=4,
                usecols=[
                    "OA11CD",
                    "LSOA11CD",
                    "All Ages"])
            fem_pop = pd.read_excel(
                xlFile,
                f"Mid-{pop_year} Females",
                header=4,
                usecols=[
                    "OA11CD",
                    "LSOA11CD",
                    "All Ages"])
        # Rename the "All Ages" columns appropriately before concating
            total_pop.rename(columns={"All Ages": "pop_count"}, inplace=True)
            males_pop.rename(columns={"All Ages": "males_pop"}, inplace=True)
            fem_pop.rename(columns={"All Ages": "fem_pop"}, inplace=True)
        # Merge the data from different sheets
            dfs_to_merge = [total_pop, males_pop, fem_pop]
            df_final = reduce(lambda left, right: pd.merge(
                left, right, on='OA11CD'), dfs_to_merge)
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
        whole_nation_pop_df.rename(
            columns={"total_pop": "pop_count"}, inplace=True)
    return whole_nation_pop_df


def get_shp_abs_path(dir):
    """Passed a directory into the function and returns the absolute path
    of where the shp file is within that directory.

    Args:
        dir (PathLike): /path/to/directory/of/shape_files.

    Returns:
        str: the absolute path of the .shp file within a directory.
    """
    files = os.listdir(dir)
    shp_files = [file for file in files if file.endswith(".shp")]
    # Add warning if there isn't a shp file in the directory
    if len(shp_files) == 0:
        raise ValueError(f"No .shp file in directory {dir}")
    shp_file = shp_files[0]

    absolute_path = os.path.join(dir, shp_file)

    return absolute_path


def get_oa_la_csv_abspath(dir):
    """Takes a directory as str and returns the absolute path of
    output area csv file.

    Args:
        dir (str): Path created with os.path.join.

    Returns:
        str: Absolute path of the csv file of the Output area.
    """
    files = os.listdir(dir)
    csv_files = [file for file in files if file.endswith(".csv")]
    csv_file = csv_files[0]

    absolute_path = os.path.join(dir, csv_file)

    return absolute_path


def _get_stops_from_api(url, file_name):
    """Gets stops data from the NaPTAN API.

    Sub function of `get_stops_file`.

    Args:
        url (str): the URL of the API endpoint.
        file_name (str): Name of the file, including extension
            to be written out containing the stops data.
    """
    # requests page
    r = requests.get(url)

    # gets content and then writes to csv
    url_content = r.content
    csv_file = open(file_name, 'wb')
    csv_file.write(url_content)
    csv_file.close()


def _get_latest_stop_file_date(dir):
    """Gets the date of the latest stop dataset.

    Args:
        dir (str): directory containing stop files.

    Returns:
        str: the date (YYYYMMDD) of the latest stop dataset.
    """
    # get's a list of files from the directory
    file_list = os.listdir(dir)

    # make sure only gets csv's
    file_list_csv = [file for file in file_list if file.endswith(".csv")]

    # files are in the format stops_YYYYMMDD
    p = re.compile(r'\d+')

    # attempts to extracts all the dates
    dates = [p.findall(i)[0] for i in file_list_csv]

    # convert to integers and get latest date
    dates_int = [int(date) for date in dates]
    dates_int.sort(reverse=True)
    latest = dates_int[0]

    return latest


def save_latest_stops_as_feather(file_name):
    """Saves the latest stop file as a feather file into
    the data folder.

    Args:
        file_name (str): file path for latest stop file.
    """
    # read in csv
    file = pd.read_csv(path_or_url(file_name),
                       usecols=config["naptan_types"].keys(),
                       dtype=config["naptan_types"])

    # get output path
    output_path = os.path.join(os.getcwd(),
                               "data",
                               "stops",
                               "Stops.feather")
    # output to feather
    file.to_feather(output_path)

    return output_path


def dl_stops_make_df(today, url):
    """Downloads the latest data from api, saves as csv & feather, returns df

    Args:
        today (str): todays date
        url (str): API URL

    Returns:
        pd.DataFrame: df of latest stops data
    """
    csv_path = os.path.join(os.getcwd(),
                            "data",
                            "stops",
                            f"stops_{today}.csv")
    # Save latest data as csv
    _get_stops_from_api(url, csv_path)
    # Save as feather
    feather_path = save_latest_stops_as_feather(csv_path)
    # Load feather as pd df
    stops_df = pd.read_feather(path_or_url(feather_path))
    return stops_df


def get_stops_file(url, dir):
    """Gets the latest stop dataset.

    If the latest stop df from the api is older then 28 days
    then function grabs a new version of file from API and
    saves this as a feather file.

    If the latest stop df from the api is less then 28 days old
    then just grabs the feather file.

    Args:
        url (str): NAPTAN API url.
        dir (str): directory where the stop data is stored.

    Returns:
        pd.DataFrame
    """

    # gets todays date and latest date of stops df
    today = int(datetime.now().strftime('%Y%m%d'))

    # gets feather stop path
    feather_path = os.path.join(os.getcwd(),
                                "data",
                                "stops",
                                "Stops.feather")
    # Check that the feather exists
    if not persistent_exists(feather_path):
        stops_df = dl_stops_make_df(today, url)
    else:  # does exist
        latest_date = _get_latest_stop_file_date(dir)
        if today - latest_date < 28:
            stops_df = pd.read_feather(path_or_url(feather_path))
        else:
            stops_df = dl_stops_make_df(today, url)

    return stops_df


def read_usual_pop_scotland(path: str):
    """Reads the usual population Scotland.

    This reads in the file containing all the
    output areas in scotland with their corresponding
    population number.

    Args:
        path (str): the path where the file exists

    Returns:
        pd.DataFrame the usual population dataframe
    """
    # reads in data and crops of header and footer
    df = pd.read_csv(path_or_url(path),
                     header=4,
                     index_col=0,
                     skipfooter=4)

    # only want OA so drop Scotland
    df_oa_only = df.drop(index=['Scotland'])

    # only use columns that we need
    essential_cols = ["All people", "Males", "Females"]
    df_essential_cols = df_oa_only[essential_cols]

    # ensure no commas in the dataset so no errors with dtypes
    for col in essential_cols:
        df_essential_cols[col] = df_essential_cols[col].str.replace(',', '')
        df_essential_cols[col] = df_essential_cols[col].astype(int)

    return df_essential_cols


def read_urb_rur_class_scotland(urb_rur_path):
    """Reads the urb/rural classification for Scotland.

    This reads in the file containing all the urban/rural class
    Then applies a mapping based on if living in a settlement >10,000
    then urban, else rural.

    Args:
        path (str): the path where the file exists

    Returns:
        pd.DataFrame: the classfication dataframe
    """
    urb_rur = pd.read_csv(path_or_url(urb_rur_path), usecols=["OA2011", "UR6_2013_2014"])

    urb_rur["urb_rur_class"] = np.where(
        (urb_rur["UR6_2013_2014"] == 1) | (
            urb_rur["UR6_2013_2014"] == 2), "urban", "rural")

    return urb_rur

def read_urb_rur_ni(urb_rur_path):
    """
    Reads small areas as urban or rural for Northern Ireland.
    Urban is classified as living in a settlement >10,000, and rural is classified
    as <10,000.
    Args:
        path (str): the path where the file exists

    Returns:
        pd.DataFrame: the urban rural classification dataframe
    """
    urb_rur = pd.read_csv(path_or_url(urb_rur_path), skiprows=3)
    urb_rur = urb_rur[['SA2011_Code', 'Settlement Classification Band']]

    # split classification bands into urban and rural
    urb_list = ['A', 'B', 'C', 'D']
    rural_list = ['E', 'F', 'G', 'H']
    urb_rur.loc[urb_rur['Settlement Classification Band'].isin(urb_list), 'urb_rur_class'] = 'urban'
    urb_rur.loc[urb_rur['Settlement Classification Band'].isin(rural_list), 'urb_rur_class'] = 'rural'

    return urb_rur

def best_before(path, number_of_days):
    """
    Checks whether a path has been modified within a period of days.

    Args:
        path (str): the path to check
        number_of_days (int): number of days previous to check for
            modifications

    Returns:
        Bool: True if path has not been modified within the number of days
            specified

    """
    todays_date = datetime.today()
    last_modified_date = datetime.fromtimestamp(os.path.getmtime(path))
    days_since_last_modification = (todays_date - last_modified_date).days

    if days_since_last_modification > number_of_days:
        expired = True
    else:
        expired = False

    return expired


def read_scottish_age(path):
    """
    Reads in the scottish age information

    Args:
        path (str): the path of the file

    Returns:
        Bool: True if path has not been modified within the number of days
            specified

    """
    # read in scottish file
    age_scotland_df = pd.read_csv(path_or_url(path), skiprows=4)

    # dropping first row as this is the whole of scotland
    age_scotland_df = age_scotland_df.iloc[1:-4, :]
    age_scotland_df = age_scotland_df.reset_index()

    return age_scotland_df


def read_ni_age_df(path):
    """
    Reads in the nothern ireland age information

    Args:
        path (str): the path of the file

    Returns:
        pd.DataFrame the age_df dataframe
    """
    # read in age df
    age_df = pd.read_excel(path_or_url(path),
                           sheet_name="SA",
                           header=5,
                           index_col="SA Code")
    # replace col names from string e.g "Age 82"
    # to just the number 82
    for col in age_df.columns:
        if bool(re.search(r"\d", col)):
            number = re.findall(r"[0-9]{1,3}", col)[0]
            age_df.rename(columns={col: number}, inplace=True)

    return age_df
