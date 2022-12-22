# Third party imports for this module
import geopandas as gpd
import numpy as np
import os
import yaml

# get current working directory
CWD = os.getcwd()

# Load config for buffers
with open(os.path.join(CWD, "config.yaml")) as yamlfile:
    config = yaml.load(yamlfile, Loader=yaml.FullLoader)
    module = os.path.basename(__file__)
    print(f"Config loaded in {module}")

# Add in capacity buffers from config
LOWERBUFFER = config["low_cap_buffer"]
UPPERBUFFER = config["high_cap_buffer"]

def get_polygons_of_loccode(geo_df: gpd.GeoDataFrame,
                            dissolveby='OA11CD',
                            search=None) -> gpd.GeoDataFrame:
    """Gets the polygon for a place based on it name, LSOA code or OA code.

    Args:
        geo_df (gpd.GeoDataFrame): Lookup geospatial data frame.
        loc_code (str): Can be one of LSOA11CD, OA11CD or LSOA11NM.
            OA11CD by default.
        search (str): Search terms to find in the LSOA11NM column. Only needed
            if intending to dissolve on a name in the LSOA11NM column.
            Defualt is None.

    Returns:
        gpd.DataFrame: GeoDataFrame with multipolygons agregated on LSOA,
            OA code, or a search in the LSOA11NM column.
    """
    if dissolveby in ['LSOA11CD', 'OA11CD']:
        polygon_df = geo_df.dissolve(by=dissolveby)
    else:
        filtered_df = geo_df[geo_df[f'{dissolveby}'].str.contains(search)]
        filtered_df.insert(0, 'place_name', search)
        polygon_df = filtered_df.dissolve(by='place_name')
    polygon_df = gpd.GeoDataFrame(polygon_df.pop('geometry'))
    return polygon_df


def buffer_points(geo_df: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    """Creates a 500m or 1000m buffer around points.
    Draws 500m if the capacity_type is low
    Draws 1000m if the capacity_type is high
    Puts the results into a new column called "geometry"
    As 'epsg:27700' projections units of km, 500m is 0.5km.
    Args:
        geo_df (gpd.DataFrame): Data frame of points to be buffered
            including a column with the capacity_type for each point.
    Returns:
        gpd.DataFrame: A dataframe of polygons create from the buffer.
    """
    geo_df['geometry'] = np.where(geo_df['capacity_type'] == "low",
                                  geo_df.geometry.buffer(LOWERBUFFER),
                                  geo_df.geometry.buffer(UPPERBUFFER))

    return geo_df


def find_points_in_poly(geo_df: gpd.GeoDataFrame, polygon_obj):
    """Find points in polygon using geopandas' spatial join
    which joins the supplied geo_df (as left_df) and the
    polygon (as right_df).

    Then drops all rows where the point is not in the polygon
    (based on column index_right not being NaN). Finally it
    drop all column names from that were created in the join,
    leaving only the columns of the original geo_df.

    Args:
        geo_df (gpg.DatFrame): a geo pandas dataframe.
        polygon_obj (str): a geopandas dataframe with a polygon column.

    Returns:
        gpd.GeoDataFrame: A geodata frame with the points inside the supplied
        polygon.
    """
    wanted_cols = geo_df.columns.to_list()
    joined_df = (gpd.sjoin
                 (geo_df,
                  polygon_obj,
                  how='left',
                  predicate='intersects'))  # op = 'within'
    filtered_df = (joined_df
                   [joined_df
                    ['index_right'].notna()])
    filtered_df = filtered_df[wanted_cols]
    return filtered_df


def points_in_polygons(points: gpd.GeoDataFrame,
                       polygons: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    """Finds points in polygons.

    A function to carry out a standard points in polygons query
    between a geodataframe of points and a geodataframe of polygons
    
    Args:
        points (gpd.GeoDataFrame): Points to be found in polygons.
        polygons (gpd.GeoDataFrame): Polygons to find points in.

    Returns:
        gpd.GeoDataFrame: Points found in polygons.
    """
    # Extract the polygon geometries, this avoids creating uneeded columns
    # in the joined dataframe
    polygons = gpd.GeoDataFrame(polygons.geometry)
    
    # Carry out points in polygons query using sjoin
    joined_df = gpd.sjoin(points,
                          polygons,
                          how='left',
                          predicate='within')
    
    return joined_df
