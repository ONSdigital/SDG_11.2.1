from typing import List
import pandas as pd
from datetime import date, datetime


def slice_age_df(df: pd.DataFrame, col_nms: List[str]):
    """Slices a dataframe according to the list of column names provided.

    Args:
        df (pd.DataFrame): DataFrame to be sliced.
        col_nms (List[str]): column names as string in a list.

    Returns:
        pd.DataFrame: A dataframe sliced down to only the columns required.
    """
    age_df = df.loc[:, col_nms]
    return age_df


def get_col_bins(col_nms: List[str]): # type hint '-> List[tuple]' causing this doctring not to work 
    """Function to group/bin the ages together in 5 year steps. 
        
    Starts the sequence at age 0. 
        Will return the ages in a list of tuples.
        The 0th position of the tuple being the lower limit
        of the age bin, the 1st position of the tuple being
        the upper limit.

    Args:
        col_nms (list of str): a list of the age columns as strings.

    Returns:
        list of tuples: a list of the ages with 5 year gaps.
    """
    
    ## Make a lists of starting and finishing indexes
    cols_start = col_nms[0::5]
    cols_fin = col_nms[4::5]
    # Generating a list of tuples which will be the age groupings
    col_bins = [(s, f) for s, f in zip(cols_start, cols_fin)]
    # Again adding "90+", doubling it so it's doubled, like the other tuples
    col_bins.append((cols_start[-1:]*2))
    # TODO: make this more intelligent. Only if there is one col name left
    # over it should be doubled.
    return col_bins


def bin_pop_ages(age_df, age_bins, col_nms):
    """ Bins the ages in the age_df in 5 year spans,
    sums the counts in those bins
    and drops the original age columns

    Args:
        df (pd.DataFrame): A dataframe of population data
            containing only the age columns.

    Returns:
        pd.DataFrame: Returns the age_df with bins.
    """
    # Grouping ages in 5 year brackets
    for bin in age_bins:
        age_df[f"{bin[0]}-{bin[1]}"] = age_df.loc[:, bin[0]:bin[1]].sum(axis=1)

    # Drop the original age columns
    age_df.drop(col_nms, axis=1, inplace=True)
    # Rename the '90+' col
    age_df.rename(columns={'90+-90+': '90+'}, inplace=True)
    # age df has now been binned and cleaned
    return age_df


def served_proportions_disagg(pop_df: pd.DataFrame,
                              pop_in_poly_df: pd.DataFrame,
                              cols_lst: List[str]):
    """Calculates the number of people in each category, as specified by the column
    (e.g age range, or disability status) who are served and not served by
    public transport, and gives those as a proportion of the total.

    Note: the numeric values in the dataframe are return as strings for
    formatting reasons

    Args:
        pop_df (pd.DataFrame): population dataframe.
        pop_in_poly_df (pd.DataFrame): dataframe resulting in the points
            in polygons enquiry to count (sum) the population within the
            service area polygon.
        cols_lst (List[str]): a list of the column names in the population
            dataframe supplied which contain population figures, and are to
            be summed and assessed as served/unserved by public transport.

    Returns:
        pd.DataFrame: a dataframe summarising
        i) the total number of people that column (e.g. age range, sex)
        ii) the number served by public transport
        iii) the proportion who are served by public transport
        iv) the proportion who are not served by public transport
    """
    # First list the age bin columns
   
    pop_sums = {}
    for col in cols_lst:
        # Total pop
        total_pop = int(pop_df[col].sum())
        # Served pop
        servd_pop = int(pop_in_poly_df[col].sum())
        # Unserved pop
        unsrvd_pop = int(total_pop-servd_pop)
        if total_pop == 0:
            # If the total population for that column is 0
            # this standard of zeros and Nones is returned 
            pop_sums[col] = {"Total": str(total_pop),
                             "Served": str(servd_pop),
                             "Unserved": str(unsrvd_pop),
                             "Percentage served": "None",
                             "Percentage unserved": "None"}
        elif total_pop > 0:
            pop_sums[col] = _calc_proprtn_srvd_unsrvd(total_pop,
                                                      servd_pop,
                                                      unsrvd_pop)

    # Make a df from the total and served pop
    tot_servd_df = pd.DataFrame(pop_sums)
    return tot_servd_df


def _calc_proprtn_srvd_unsrvd(total_pop,
                                   servd_pop,
                                   unsrvd_pop):
    """Calculates proportions of people served and unserved by public transport.

    Args:
        total_pop (int):  The total population for that category.
        servd_pop (int):  Of the population, those who are served
            by public transport.
        unsrvd_pop (int): Of the population, those who are NOT served
            by public transport.

    Returns:
        dict: A dictionary with the following:
                i) the total number of people that column (e.g. age range, sex)
                ii) the number served by public transport
                iii) the percentage of who are served by public transport
                iv) the percentage ofwho are not served by public transport
    """
    # Get proportion served
    pct_servd = round((servd_pop/total_pop)*100, 2)
    # Get proportion unserved
    pct_unserved = round(((total_pop-servd_pop)/total_pop)*100, 2)
    results_dict = {"Total": str(total_pop),
                        "Served": str(servd_pop),
                        "Unserved": str(unsrvd_pop),
                        "Percentage served": str(pct_servd),
                        "Percentage unserved": str(pct_unserved)}
    return results_dict


def highly_serv_stops(region):
    """Retrieves timetable data from the Traveline National Dataset for
    any region. Filters stops with that have >1 departure per hour
    on a weekday (Wed is default) between 6am and 8pm.

    Args:
        region (str): the name of the region of the UK that the data
            is needed for.
    """
    #  day="Wed"
    return None


def filter_stops(stops_df):
    """Filters the stops dataframe based on two things:
    
    | 1) Status column - We want to keep stops which are active, pending or new.
    | 2) StopType want only to include bus and rail stops.

    Args:
        stops_df (pd.DataFrame): the dataframe to filter.

    Returns:
        pd.DataFrame: Filtered_stops which meet the criteria
            of keeping based on status/stoptype columns.
    """
    # stop_types we would like to keep within the dataframe
    stop_types = ["RSE","RLY","RPL","TMU","MET","PLT",
                "BCE", "BST","BCQ", "BCS","BCT"]

    filtered_stops = stops_df[(stops_df["Status"] == "active") |
                           (stops_df["Status"] == "pending") |
                           (stops_df["Status"] == None) |
                           (stops_df["Status"] == "new")]

    boolean_stops_type = filtered_stops["StopType"].isin(stop_types) 
    filter_stops=filtered_stops[boolean_stops_type]

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
    dictionary_map = {"RSE": "high",
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

    stops_df["capacity_type"]=stops_df["StopType"].map(dictionary_map)

    return stops_df

def disab_disagg(disability_df,la_pop_df):
    """Calculates number of people in the population that are classified as 
        disabled or not disabled and this is merged onto the local authority 
        population dataframe.
        
    Args:
        disability_df (pd.DataFrame): Dataframe that includes disability estimates for 
                                    each output area.
        la_pop_df (gpd.GeoDataFrame): GeoPandas Dataframe that includes 
                                output area codes and population estimates.

    Returns:
        gpd.GeoDataFrame: GeoPandas Dataframe that includes population estimates,
                        geographical location, and proportion of disabled/non-disabled 
                        for each output area in the local authority chosen.
    """
    # Getting the disab total
    disability_df["disb_total"] = (disability_df["disab_ltd_lot"]
                                   + disability_df["disab_ltd_little"])

    # Calcualting the total "non-disabled"
    la_pop_only = la_pop_df[['OA11CD','pop_count']]
    disability_df = la_pop_only.merge(disability_df, on="OA11CD")
    # Putting the result back into the disability df
    disability_df["non-disabled"] = disability_df["pop_count"] - disability_df['disb_total']

    # Calculating the proportion of disabled people in each OA
    disability_df["proportion_disabled"] = (
        disability_df['disb_total']
        /
        disability_df['pop_count']
    )
    
    # Calcualting the proportion of non-disabled people in each OA
    disability_df["proportion_non-disabled"] = (
        disability_df['non-disabled']
        /
        disability_df['pop_count']
    )
    
    # Slice disability df that only has the proportion disabled column and the OA11CD col
    disab_prop_df = disability_df[['OA11CD', 'proportion_disabled', 'proportion_non-disabled']]

    # Merge the proportion disability df into main the pop df with a left join
    la_pop_df = la_pop_df.merge(disab_prop_df, on='OA11CD', how="left")

    # Make the calculation of the number of people with disabilities in the year
    # of the population estimates
    la_pop_df["number_disabled"] = (
        round
        (la_pop_df["pop_count"]
         *
         la_pop_df["proportion_disabled"])
    )
    la_pop_df["number_disabled"] = la_pop_df["number_disabled"].astype(int)

    # Make the calculation of the number of non-disabled people in the year
    # of the population estimates
    la_pop_df["number_non-disabled"] = (
        round
        (la_pop_df["pop_count"]
         *
         la_pop_df["proportion_non-disabled"])
    )
    la_pop_df["number_non-disabled"] = la_pop_df["number_non-disabled"].astype(int)
    return la_pop_df


def filter_bus_timetable_by_day(bus_timetable_df, day):
    """
    Extract serviced bus stops based on specific day of the week.

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
        bus_timetable_df (pandas dataframe): df to filter
        day (str) : day of the week in title case, e.g. "Wednesday"

    Returns:
        pd.DataFrame: filtered pandas dataframe   
    """
    # Measure the dataframe
    original_rows = bus_timetable_df.shape[0]

    # Count the bus services
    orig_service_count = bus_timetable_df.service_id.unique().shape[0]

    # Get the minimum date range
    earliest_start_date = bus_timetable_df.start_date.min()
    latest_end_date = bus_timetable_df.end_date.max()


    # Identify days in the range and count them
    date_range = pd.date_range(earliest_start_date, latest_end_date)
    date_day_couplings_df = pd.DataFrame({"date": date_range,
                                         "day_name": date_range.day_name()})
    days_counted = date_day_couplings_df.day_name.value_counts()
    days_counted_dict = days_counted.to_dict()

    # Validate user choices
    if day not in days_counted_dict.keys():
        raise KeyError("The day chosen in not available. Should be a weekday in title case.")
    max_ord = days_counted_dict[day]
    ord = round(max_ord/2)
    
    # filter all the dates down the to the day needed
    day_filtered_dates = (date_day_couplings_df
                          [date_day_couplings_df.day_name == day])

    # Get date of the nth (ord) day
    nth = ord - 1
    date_of_day_entered = day_filtered_dates.iloc[nth].date

    # Filter the bus_timetable_df 
    bus_timetable_df = bus_timetable_df[(bus_timetable_df['start_date']
                                         <= date_of_day_entered) & 
                                        (bus_timetable_df['end_date']
                                         >= date_of_day_entered)]
    
    # Print date being used (consider logging instead)
    day_date = date_of_day_entered.date()
    print(f"The date of {day} number {ord} is {day_date}")

    # Print how many rows have been dropped (consider logging instead)
    print(f"Selecting only services covering {day_date} reduced records by {original_rows-bus_timetable_df.shape[0]} rows")

    # Print how many bus services are in the analysis and how many were dropped
    service_count = bus_timetable_df.service_id.unique().shape[0]
    dropped_services = orig_service_count - service_count
    print(f"There are {service_count} bus services in the analysis")
    print(f"Filtering by day has reduced services by {dropped_services}")


    return bus_timetable_df


def filter_by_year(df, year="2022"):
    """Filter the timetable_df by year
    
    Args:
        bus_timetable_df (pandas dataframe): df to filter
        day (str) : day of the week in title case, e.g. "Wednesday"
    
    Returns:
        pd.DataFrame: pandas dataframe filtered by year specified   
    """
    # Measure the dataframe
    original_rows = df.shape[0]

    # Create string of start and end date of given year
    start_year = f'{year}0101'
    end_year = f'{year}1231'

    # Create filter condition
    start_end_date_during_year = ((df['start_date'] >= start_year)
                                  & (df['start_date'] <= end_year)
                                  & (df['end_date'] >= start_year)
                                  & (df['end_date'] <= end_year))
    # Filter df
    df = df.loc[start_end_date_during_year]
    
    # Print how many rows have been dropped (consider logging instead)
    print(f"Filtering by year dropped {original_rows-df.shape[0]} rows")
    return df