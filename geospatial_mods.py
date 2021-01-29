# Third party imports for this module
import geopandas as gpd


def get_polygons_of_loccode(geo_df, dissolveby='OA11CD', search=None):
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


def buffer_points(geo_df, distance_km=500):
    """
    Provide a Geo Dataframe with points you want buffering.
    Draws a 5km (radius) buffer around the points.
    Puts the results into a new column called "buffered"
    As 'epsg:27700' projections units of km, 500m is 0.5km.
    """
    geo_df['geometry'] = geo_df.geometry.buffer(distance_km)
    return geo_df


def draw_5km_buffer(centroid):
    """
    Draws a 5km (radius) buffer around a point. As 'epsg:27700' projections
    units of km so 500m is 0.5km.
    """
    distance_km = 0.5
    return centroid.buffer(distance=distance_km)


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
