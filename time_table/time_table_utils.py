"""All functions realted to the bus and train timetable data."""

import logging

import pandas as pd

# Create logger
logger = logging.getLogger(__name__)

def filter_stops(stops_df):
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


def add_stop_capacity_type(stops_df):
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

def filter_timetable_by_day(timetable_df, day):
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