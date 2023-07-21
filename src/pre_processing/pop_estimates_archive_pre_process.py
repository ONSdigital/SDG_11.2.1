# """This program is used to pre-process the population estimates archive data.
# The archived data is in a different format than the current data. It has many years's data in one file for each region.
# Also the data is a long format, so it needs to be pivoted to a wide format."""

import re
import pathlib as pl
import pandas as pd
import sys
from glob import glob


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

if __name__ == "__main__":

    # Define the input and output file paths
    input_folder = "data/population_estimates/2002-2012"

    # Define the years to process
    years = list(range(2002, 2013))

    # Define an empty dictionary to store the dataframes for each year
    year_data = {}

    # Loop through each year and process the data for all regions
    for year in years:

        year_col = f"Population_{year}"

        # Define an empty dictionary to store the data for each region
        region_data = {}

        # Loop through each file in the input folder
        for filename in glob(input_folder):
            if filename.endswith(".csv"):
                # Load the CSV file into a dataframe
                single_region_df = pd.read_csv(pl.Path.joinpath(input_folder, filename),
                                               usecols=["OA11CD", "LAD11CD", "Age", "Sex", year_col])

                # Extract the region code from the filename
                region_code = extract_region(filename)

                # Extract the data for the specified year and store it in the region_data dictionary
                region_data[region_code] = single_region_df

        # Concatenate the data for all regions into a single dataframe
        sex_in_one_year_df = pd.concat(region_data.values())
        del region_data
        
        # Get the three sex disaggregated dataframes
        # Call the function to get the dataframes
        both_sexes_df, male_df, female_df = age_pop_one_year(sex_in_one_year_df, year)

        # Pivot the dataframe to a wide format
        # grouped_df = df.groupby(["OA11CD", "Age"]).sum().pivot_table(index="OA11CD", columns="Age", values="Population_2002")

        both_sexes_df, male_df, female_df = pivot_year_dfs(
            year_col, both_sexes_df, male_df, female_df)

        # Create a dictionary with the dataframes
        data_by_sex = {"both_sexes_df": both_sexes_df,
                       "male_df": male_df, "female_df": female_df}

        # Create the output folder if it doesn't exist
        output_folder = pl.Path(f"data/population_estimates/{year}")
        if not output_folder.exists:
            output_folder.mkdir(parents=True)

        # Write year_df to a feather file
        for name, sex_in_one_year_df in data_by_sex.items():
            # Stringify the column names (because feather doesn't like ints as column names)
            sex_in_one_year_df.rename(columns={col: str(col)
                           for col in sex_in_one_year_df.columns}, inplace=True)
            
            # Write the dataframe to a feather file
            file_path = make_feather_path(year, output_folder, name)
            
            try:
                sex_in_one_year_df.reset_index().to_feather(file_path)
            except ValueError or FileNotFoundError as e:
                print(e)
                sys.exit(1)
                
        del data_by_sex
