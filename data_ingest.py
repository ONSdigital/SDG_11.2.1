import os
import requests
import pandas as pd
from shapely.geometry import Point
import geopandas as gpd

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
        print(f"Deleting {zip_name} from {zip_path}")
        os.remove(zip_path)

    return True

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
