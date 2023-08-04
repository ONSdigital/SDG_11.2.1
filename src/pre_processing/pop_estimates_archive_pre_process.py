# """This program is used to pre-process the population estimates archive data.
# The archived data is in a different format than the current data. It has many years's data in one file for each region.
# Also the data is a long format, so it needs to be pivoted to a wide format."""

import re
import pathlib as pl
import pandas as pd
import sys
from glob import glob
import duckdb

# %%
db_output = "data/population_estimates/2002-2012/pop_est_2002-2012.db"


def extract_region(file_name):
    # Extact the region name from the file name
    region_pattern = r"unformatted-(.*?)-mid2002"
    match = re.search(region_pattern, file_name)

    if match:
        region = match.group(1)

    return region


def pivot_year_dfs(year_col, *year_dfs):
    """This function pivots the dataframes to a wide format and
        adds a All Ages column to each dataframe which sums the population
        for all ages across each OA"""
    pivoted_dfs = []
    for year_df in year_dfs:
        pivoted_df = year_df.pivot_table(index=["OA11CD", "LAD11CD"], columns=[
                                         "Age"], values=year_col, aggfunc="sum")
        pivoted_df["All Ages"] = pivoted_df.sum(axis=1)
        pivoted_dfs.append(pivoted_df)
    return tuple(pivoted_dfs)


def make_feather_path(year, output_folder, name):
    file_path = pl.Path.joinpath(output_folder, f"pop_est_{name}_all_regions_{year}.feather")
    return file_path


def age_pop_one_year(combined_sexes_df, year):
    """Splits the data into three sex groups and drops the sex column.
    
    This function takes a long (not wide) dataframe and returns three dataframes
    long dataframes. 
    """

    cols = ["OA11CD", "Age", "LAD11CD", f"Population_{year}"]

    # get single sex dfs
    both_sexes_df = combined_sexes_df[combined_sexes_df.Sex == 1 | 2][cols]
    male_df = combined_sexes_df[combined_sexes_df.Sex == 1][cols]
    female_df = combined_sexes_df[combined_sexes_df.Sex == 2][cols]

    return both_sexes_df, male_df, female_df

def load_region_data(file_path, year_cols):
    """Load the data for a single region and return a dataframe."""
        
    # load all years for one region
    single_region_df = pd.read_csv(file_path, usecols=["OA11CD",
                                                        "LAD11CD",
                                                        "Age",
                                                        "Sex",
                                                        *year_cols])
    region_code = extract_region(file_path)
    
    # Create a column called region
    single_region_df["region"] = region_code
    
    # Reorder the columns
    ordered_cols = ["OA11CD", "LAD11CD", "Age", "Sex", "region"]+year_cols
    single_region_df = single_region_df[ordered_cols]
    
    return single_region_df


def create_output_folder(year: int) -> pl.Path:
    """Create the output folder for a given year if it doesn't exist."""
    output_folder = pl.Path(f"data/population_estimates/{year}")
    if not output_folder.exists():
        output_folder.mkdir(parents=True)
    return output_folder


def stringify_column_names(df):
    """Stringify the column names of a DataFrame."""
    df.rename(columns={col: str(col) for col in df.columns}, inplace=True)

# %%
def main():

    # Define the input and output file paths
    input_folder = "data/population_estimates/2002-2012"

    # Define the years to process
    years = list(range(2002, 2013))

    # Define an empty dictionary to store the dataframes for each year
    year_data = {}

    # Loop through each year and process the data for all regions


    # Define the column name for the year
    year_cols = [f"Population_{year}" for year in years]
    
    column_types = {
    "OA11CD": "TEXT",
    "LAD11CD": "TEXT",
    "Age": "TEXT",
    "Sex": "TEXT",
    "Population_2002": "INTEGER",
    "Population_2003": "INTEGER",
    "Population_2004": "INTEGER",
    "Population_2005": "INTEGER",
    "Population_2006": "INTEGER",
    "Population_2007": "INTEGER",
    "Population_2008": "INTEGER",
    "Population_2009": "INTEGER",
    "Population_2010": "INTEGER",
    "Population_2011": "INTEGER",
    "Population_2012": "INTEGER"
}
    
    # Create a connection to the database
    con = duckdb.connect(db_output)

    # Create a query to load all the csv data in one go
    query = f"""
    CREATE TABLE IF NOT EXISTS all_csvs
    AS SELECT * 
    FROM read_csv_auto('data/population_estimates/2002-2012/*.csv',
    header=true,
    columns={column_types})"""
    
    # Run query to load all the csv data in one go and create the table
    con.execute(query)
    




    # Get the data for all regions for one year using year_cols[0]
    # Single year_data cols
    for year in years:
        
        year_col = f"Population_{year}"
        
        single_yr_cols = ["OA11CD", "LAD11CD", "Age", "Sex", year_col]
        columns = ", ".join(single_yr_cols)
        all_data_for_one_year = con.execute(f"SELECT {columns} FROM all_csvs LIMIT 10").df()
        
        ##### THIS IS WHERE I GOT TO #####
        
        # TODO: create a duckdb version of the following functions including filtering by sex
        # and pivoting the data to a wide format
        # and writing to a feather file
        
                
        # Get the three sex disaggregated dataframes
        # Call the function to get the dataframes
        both_sexes_df, male_df, female_df = age_pop_one_year(all_data_for_one_year, year)

        # Pivot the dataframe to a wide format
        # grouped_df = df.groupby(["OA11CD", "Age"]).sum().pivot_table(index="OA11CD", columns="Age", values="Population_2002")

        both_sexes_df, male_df, female_df = pivot_year_dfs(
            year_col, both_sexes_df, male_df, female_df)

        # Create a dictionary with the dataframes
        data_by_sex = {"both_sexes_df": both_sexes_df,
                        "male_df": male_df,
                        "female_df": female_df}

        output_folder= create_output_folder(year)
    
    # Write year_df to a feather file
    for name, sex_in_one_year_df in data_by_sex.items():
        # Stringify the column names (because feather doesn't like ints as column names)
        stringify_column_names(sex_in_one_year_df)
        
        # Write the dataframe to a feather file
        file_path = make_feather_path(year, output_folder, name)
        
        try:
            sex_in_one_year_df.reset_index().to_feather(file_path)
        except ValueError or FileNotFoundError as e:
            print(e)
            sys.exit(1)

if __name__ == "__main__":
    main()