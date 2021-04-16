import pandas as pd

def gen_age_col_lst():
    """Makes the column names for the population df. Names are numbers 0-89 and 90+
        all as strings. 

    Returns:
        list of str: list of the column names for all age count columns in the 
        population dataframe 
    """    
    # Getting a list of columns names (0-90) which are strings of integers
    age_col_lst = [str(n) for n in range(90)]
    # Adding '90+' to complete the list
    age_col_lst.append('90+')
    return age_col_lst


def slice_age_df(df, col_nms):
    """Slices a dataframe according to the list of column names provided.

    Args:
        df (pd.DataFrame): DataFrame to be sliced
        col_nms (list of str): column names as string in a list

    Returns:
        pd.DataFrame: A dataframe sliced down to only the columns required."""
    age_df = df.loc[:, col_nms]
    return age_df


def get_col_bins(col_nms):
    """Groups/bins the ages, with 5 year step, starting at "0" into a list 
        of tuples. Depends on yada yada  

    Args:
        col_nms (list of str): a list of the age columns as strings

    Returns:
        list of tuples: a list of the ages with 5 year gaps
    """
    # Make a lists of starting and finishing indexes
    cols_start = col_nms[0::5]
    cols_fin = col_nms[4::5]
    # Generating a list of tuples which will be the age groupings
    col_bins = [(s, f) for s, f in zip(cols_start, cols_fin)]
    # Again adding "90+", doubling it so it's doubled, like the other tuples
    col_bins.append((cols_start[-1:]*2))
    return col_bins


def bin_pop_ages(age_df, age_bins, col_nms):
    """ Bins the ages in the age_df in 5 year spans,
            sums the counts in those bins
            and drops the original age columns

    Args:
        df (pd.DataFrame): A dataframe of population data
            containing only the age columns
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

def served_proportions_disagg(pop_df, pop_in_poly_df, cols_lst):
    """Calculates the number of people in each category, as specified by the column
        (e.g age range, or disability status) who are served and not served by public 
        transport, and gives those as a proportion of the total. 

    Parameters:
        pop_df (pd.DataFrame) : population dataframe

        pop_in_poly_df (pd.DataFrame) : dataframe resulting in the points
            in polygons enquiry to count (sum) the population within the
            service area polygon.  

        cols_lst (list): a list of the column names in the population 
            dataframe supplied which are to be summed and assessed for
            as served/unserved by public transport

    Returns:
        pd.DataFrame: a dataframe summarising
        i) the total number of people in each age bin
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
        # Get proportion served
        portion_srvd = servd_pop/total_pop
        # Get proportion unserved
        unserved = (total_pop-servd_pop)/total_pop
        pop_sums[col] = {"total":total_pop,
                            "Served":servd_pop,
                            "Unserved":unsrvd_pop,
                            "Proportion served": round(portion_srvd,4),
                            "Prportion unserved":round(unserved,4)}
    # Make a df from the total and served pop
    tot_servd_df = pd.DataFrame(pop_sums)
    return tot_servd_df



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
    #  day="Wed"
    return None 
