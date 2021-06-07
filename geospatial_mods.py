# Third party imports for this module
import geopandas as gpd
import pandas as pd
from shapely.ops import unary_union


def get_polygons_of_loccode(geo_df: gpd.GeoDataFrame,
                            dissolveby='OA11CD',
                            search=None) -> gpd.GeoDataFrame:
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


def demarc_urb_rural(urbDef, ):
    """
    Creates spatial clusters of urban environments based on specified
        definition of 'urban'.
        - 'engwls' for the English/Welsh definition of urban
        - 'scott' for the Scottish definition of urban
        - 'euro' for the European definition of urban

    Parameters:
        urbDef (str): the definition of urban to be used
    Returns: TBC (probably a polygon)
            """

    return None


def buffer_points(geo_df, metres=500):
    """
    Provide a Geo Dataframe with points you want buffering.
    Draws a 5km (radius) buffer around the points.
    Puts the results into a new column called "buffered"
    As 'epsg:27700' projections units of km, 500m is 0.5km.
    """
    geo_df['geometry'] = geo_df.geometry.buffer(metres)
    return geo_df

# TODO: remove this if it's junk code
# def draw_5km_buffer(centroid):
#     """
#     Draws a 5km (radius) buffer around a point. As 'epsg:27700' projections
#     units of km so 500m is 0.5km.
#     """
#     distance_km = 0.5
#     return centroid.buffer(distance=distance_km)


def find_points_in_poly(geo_df, polygon_obj):
    """Find points in polygon using geopandas' spatial join
        which joins the supplied geo_df (as left_df) and the
        polygon (as right_df).

        Then drops all rows where the point is not in the polygon
        (based on column index_right not being NaN). Finally it
        drop all column names from that were created in the join,
        leaving only the columns of the original geo_df

        Arguments:
            geo_df (gpg.DatFrame): a geo pandas dataframe
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


def poly_from_polys(geo_df):
    """Makes a combined polygon from the multiple polygons in a geometry
        column in a geo dataframe.

    Args:
        geo_df (gpd.DataFrame):

    Returns:
        class Polygon : a combined polygon which is the perimter of the
            polygons provided.
    """
    poly = unary_union(list(geo_df.geometry))
    return poly


def ward_nrthng_eastng(district, ward):
    # TODO: finish this function doctring
    """Gets the eastings and northings of a ward in a metropolitan area
    Args:
        district (str): The district geo code
        ward (str): The ward geo code

    Returns:
        [type]: [description]
    """
    csvurl = f"https://www.doogal.co.uk/AdministrativeAreasCSV.ashx?district={district}&ward={ward}"
    df = pd.read_csv(csvurl, usecols=['Easting', 'Northing'])
    eastings = [easting for easting in df.Easting]
    northings = [northing for northing in df.Northing]
    mins_maxs = {
        "e_min": min(eastings),
        "e_max": max(eastings),
        "n_min": min(northings),
        "n_max": max(northings)}
    return mins_maxs


def filter_stops_by_ward(df, mins_maxs):
    """Makes a filtered dataframe (used for the filtering the stops dataframe)
        based on northings and eastings.

    Args:
        df (pd.DataFrame): The full dataframe to be filtered
        mins_maxs (dict): A dictionary with the mins and maxes of the eastings
            and northings of the area to be filtered

    Returns:
        pd.DataFrame : A filtered dataframe, limited by the eastings and
            northings supplied
    """
    # Limit the stops, filtering by the min/max eastings/northings for ward
    mm = mins_maxs
    nrth_mask = (mm['n_min'] < df['Northing']) & (df['Northing'] < mm['n_max'])
    east_mask = (mm['e_min'] < df['Easting']) & (df['Easting'] < mm['e_max'])

    # Filter the stops for the ward
    filtered_df = df[nrth_mask & east_mask]

    return filtered_df
