"""All functions realted to the bus and train timetable data."""

import logging
import pandas as pd
from typing import List, Tuple

# Create logger
logger = logging.getLogger(__name__)


def filter_stops(stops_df: pd.DataFrame) -> pd.DataFrame:
    """Filters the stops dataframe based on two things:

    | 1) Status column. We want to keep stops which are active, pending or new.
    | 2) StopType want only to include bus and rail stops.

    Args:
        stops_df (pd.DataFrame): the dataframe to filter.

    Returns:
        pd.DataFrame: Filtered_stops which meet the criteria
            of keeping based on status/stoptype columns.
    """
    # stop_types we would like to keep within the dataframe
    stop_types = ["RSE", "RLY", "RPL", "TMU", "MET", "PLT",
                  "BCE", "BST", "BCQ", "BCS", "BCT"]

    # Filter the stops based on the status column (active, pending, new and None)
    filtered_stops = stops_df[(stops_df["Status"] == "active") |
                              (stops_df["Status"] == "pending") |
                              (stops_df["Status"] is None) |
                              (stops_df["Status"] == "new")]

    # Filter the stops based on the stop types (bus and rail)
    boolean_stops_type = filtered_stops["StopType"].isin(stop_types)
    filter_stops = filtered_stops[boolean_stops_type]

    return filter_stops


def add_stop_capacity_type(stops_df: pd.DataFrame) -> pd.DataFrame:
    """Adds capacity_type column.

    Column is defined with the following dictionary using the StopType
    Bus stops are low capacity, train stations are high capacity.

    Args:
        stops_df (pd.DataFrame): The dataframe to add the column to.

    Returns:
        pd.DataFrame: dataframe with new capacity_type column.
    """
    # Create a dictionary to map the StopType to capacity level
    capacity_map = {"RSE": "high",
                    "RLY": "high",
                    "RPL": "high",
                    "TMU": "high",
                    "MET": "high",
                    "PLT": "high",
                    "BCE": "low",
                    "BST": "low",
                    "BCQ": "low",
                    "BCS": "low",
                    "BCT": "low"}

    # Add the capacity_type column to the stops dataframe
    stops_df["capacity_type"] = stops_df["StopType"].map(capacity_map)

    return stops_df


def filter_timetable_by_day(timetable_df: pd.DataFrame, day: str) -> pd.DataFrame:
    """Extract serviced stops based on specific day of the week.

    The day is selected from the available days in the date range present in
      timetable data.

    1) identifies which days dates in the entire date range
    2) counts days of each type to get the maximum position order
    3) validates user's choice for `day` - provides useful errors
    4) creates ord value that is half of maximum position order to ensure
    as many services get included as possible.
    4) selects a date based on the day and ord parameters
    5) filters the dataframe to that date

    Args:
        timetable_df (pandas dataframe): df to filter
        day (str) : day of the week in title case, e.g. "Wednesday"

    Returns:
        pd.DataFrame: filtered pandas dataframe
    """
    # Measure the dataframe
    original_rows = timetable_df.shape[0]

    # Count the services
    orig_service_count = timetable_df.service_id.unique().shape[0]

    # Get the minimum date range
    earliest_start_date = timetable_df.start_date.min()
    latest_end_date = timetable_df.end_date.max()

    # Identify days in the range and count them
    date_range = pd.date_range(earliest_start_date, latest_end_date)
    date_day_couplings_df = pd.DataFrame({"date": date_range,
                                         "day_name": date_range.day_name()})
    days_counted = date_day_couplings_df.day_name.value_counts()
    days_counted_dict = days_counted.to_dict()

    # Validate user choices
    if day not in days_counted_dict.keys():
        raise KeyError(
            """The day chosen in not available.
            Should be a weekday in title case.""")
    # Get the maximum position order (ordinal)
    max_ord = days_counted_dict[day]
    ord = round(max_ord / 2)

    # Filter all the dates down the to the day needed
    day_filtered_dates = (date_day_couplings_df
                          [date_day_couplings_df.day_name == day])

    # Get date of the nth (ord) day
    nth = ord - 1
    date_of_day_entered = day_filtered_dates.iloc[nth].date

    # Filter the timetable_df by date range
    timetable_df = timetable_df[(timetable_df['start_date']
                                 <= date_of_day_entered) &
                                (timetable_df['end_date']
                                 >= date_of_day_entered)]

    # Then filter to day of interest
    timetable_df = timetable_df[timetable_df[day.lower()] == 1]

    # Filter the timetable_df by date range
    timetable_df = timetable_df[(timetable_df['start_date']
                                 <= date_of_day_entered) &
                                (timetable_df['end_date']
                                 >= date_of_day_entered)]

    # Then filter to day of interest
    timetable_df = timetable_df[timetable_df[day.lower()] == 1]

    # Print date being used (consider logging instead)
    day_date = date_of_day_entered.date()
    logger(f"The date of {day} number {ord} is {day_date}")

    # Print how many rows have been dropped (consider logging instead)
    logger(
        f"Selecting only services covering {day_date} reduced records"
        f"by {original_rows-timetable_df.shape[0]} rows"
    )

    # Print how many services are in the analysis and how many were dropped
    service_count = timetable_df.service_id.unique().shape[0]
    dropped_services = orig_service_count - service_count
    logger(f"There are {service_count} services in the analysis")
    logger(f"Filtering by day has reduced services by {dropped_services}")

    return timetable_df


def extract_msn_data(msn_file: str) -> List[List]:
    """Extract data from the msn file.

    Args:
        msn_file (msn): A text file containing the msn data.

    Returns:
        list: A list of lists containing the msn data.
    """

    # Store msn data
    msn_data_lst = []

    with open(msn_file, 'r') as msn_data:
        # Skip header
        next(msn_data)
        for line in msn_data:
            # Only interested in rows starting with A.
            # Rows starting with L display aliases of station names
            # Stripping the values because some are padded out with blank spaces
            # as part of the file format.
            # Coordinate data provided is actually the grid reference
            # but without the 100km square (two letters at the start) so
            # very difficult to extract coordinates. Hence, will add in
            # coordinate data from an external source.
            # NB tiploc_code is unique, but crs_code isnt.
            if line.startswith('A'):
                station_name = line[5:31].strip()
                tiploc_code = line[36:43].strip()
                crs_code = line[49:52].strip()

                msn_data_lst.append([station_name,
                                     tiploc_code,
                                     crs_code])
    return msn_data_lst

# add type hints
def extract_mca(mca_file: str) -> Tuple[List[List], List[List]]:
    """Extract data from the mca file.

    The logic for this extraction is as follows:
        Each new journey starts with "BS". Within this journey we have
        * multiple stops
        * LO is origin
        * LI are inbetween stops
        * LT is terminating stop
        * Then a new journey starts with BS again
        Within each journey are a few more lines that we can ignore e.g.
        * BX = extra details of the journey
        * CR = changes en route. Doesnt contain any arrival / departure times.

    Process:
    * Starts by finding all the schedules within the file. 
    * Extract relevant information into the journey dataframe, and then copy unique_id
    * onto all trips within that journey.

    Args:
        mca_file (_type_): _description_


    Returns:
        schedules (list): list of lists containing schedule information 
            ready for dataframe
        stops (list): list of lists containing stop information
            ready for dataframe
    """
    # Create a flag that specifies when we have found a new journey
    journey = False

    # Store schedule information
    schedules = []
    # Store stop information
    stops = []

    with open(mca_file, 'r') as mca_data:
        # Skip the header
        next(mca_data)
        for line in mca_data:
            # A schedule is started by a record beginning with BS.
            # Other entries exist but are not needed for our purpose.
            # Schedules are then further broken down by transaction type
            # (N - new, R - revised, D - delete)
            # Ignore any that are transaction type delete.
            if line[0:3] == 'BSN' or line[0:3] == 'BSR':
                # Switch flag on as we have found a journey
                journey = True

            # Get unique ID for schedule
            # ID in dataset is not actually unique as same train has several
            # schedules with different dates and calendars. Create ID from
            # these variables.
                schedule_id = line[3:9] + line[9:15] + line[15:21] + line[21:28]

            # Extract start and end date of service (yymmdd)
                start_date = line[9:15].strip()
                end_date = line[15:21].strip()

            # Extract the calender information for this journey
            # i.e. what days of the week it runs
            # Only interested in weekdays for the timebeing.
                monday = int(line[21].strip())
                tuesday = int(line[22].strip())
                wednesday = int(line[23].strip())
                thursday = int(line[24].strip())
                friday = int(line[25].strip())

            # Store data to be added to the dataframe
                schedules.append([schedule_id,
                                  start_date,
                                  end_date,
                                  monday,
                                  tuesday,
                                  wednesday,
                                  thursday,
                                  friday])

            # Skip as this is all we need to do for an entry starting
            # with BS
                continue

        # If we have found a new journey, go through the lines that are
        # part of this jounrey and give them the schedule id.

        # Journeys split into origin (LO), stops (LI) and terminus (LT)
        # CR (changes en route) and BX (extra schedule details) can be
        # ignored as not relevant to our purpose.

        # If station has a departure time extract time, tiploc_code and
        # activity type.

        # NB times can end on a H sometimes which indicates a half minute
        # Rather than rounding up and down, just ignoring this for the moment
        # and taking only first four characters (hh:mm)

            if journey:
                if line[0:2] == 'LO':
                    departure_time = line[10:14].strip()
                    tiploc_code = line[2:10].strip()
                    activity_type = line[29:41].strip()

                elif line[0:2] == 'LI':
                    departure_time = line[15:19].strip()
                    tiploc_code = line[2:10].strip()
                    activity_type = line[42:54].strip()

                elif line[0:2] == 'LT':
                    departure_time = line[15:19].strip()
                    tiploc_code = line[2:10].strip()
                    activity_type = line[25:37].strip()

                # As we know that LT signifies the last stop in a journey
                # and we have extracted everything we need we now switch the
                # flag off
                    journey = False
                else:
                    # Skipping BR and CX
                    continue

                # Store data to be added to dataframe
                stops.append([schedule_id,
                              departure_time,
                              tiploc_code,
                              activity_type])
    return schedules, stops
