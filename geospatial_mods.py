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


def buffer_points(geo_df: gpd.GeoDataFrame, metres=int(500)) -> gpd.GeoDataFrame:
    """
    Provide a Geo Dataframe with points you want buffering.
    Draws a 5km (radius) buffer around the points.
    Puts the results into a new column called "buffered"
    As 'epsg:27700' projections units of km, 500m is 0.5km.
    """
    geo_df['geometry'] = geo_df.geometry.buffer(metres)
    return geo_df

def find_points_in_poly(geo_df: gpd.GeoDataFrame, polygon_obj):
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

# TODO: delete this function if it is no longer needed.
# def poly_from_polys(geo_df):
#     """Makes a combined polygon from the multiple polygons in a geometry
#         column in a geo dataframe.

#     Args:
#         geo_df (gpd.DataFrame):

#     Returns:
#         class Polygon : a combined polygon which is the perimter of the
#             polygons provided. (shapely.geometry.polygon.Polygon)
#     """
#     poly = unary_union(list(geo_df.geometry))
#     return poly


def ward_nrthng_eastng(district: str, ward: str):
    # TODO: finish this function doctring
    """Gets the eastings and northings of a ward in a metropolitan area
    
    THIS WILL GET DELETED FOR VERSION 1.0
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


