import os
import json
from zipfile import ZipFile

def highly_serv_stops(region):
    """
    Retrieves timetable data from the Traveline National Dataset for
        any region. Filters stops with that have >1 departure per hour
        on a weekday (Wed is default) between 6am and 8pm.
    Parameters: 
        region (str): the name of the region of the UK that the data
            is needed for
    Returns:
        highly serviced stops for region    
    """
    day="Wed"
   return None 
