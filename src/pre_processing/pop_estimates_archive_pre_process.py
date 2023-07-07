"""This program is used to pre-process the population estimates archive data. 
The archived data is in a different format than the current data. It has many years's data in one file for each region.
Also the data is a long format, so it needs to be pivoted to a wide format."""

import os
import pandas as pd
import glob
import re
import sys
import pickle
import logging
from typing import Generator, Tuple

import gptables as gpt

# add the parent directory to the path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import data_ingest as di


logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')


# Define the directory path where the CSV files are located
directory_path = '/home/james/programming_projects/work/SDG_11.2.1/data/population_estimates/2002-2012'


# Initialize empty dictionaries to store the dataframes
all_regions_by_year = {}
all_ages_df = {}
male_df = {}
female_df = {}
dfs_dict = {}

# Create a generator of years
def years_generator():
    return map(str, list(range(2002, 2013)))

# Get a generator of all CSV files in the directory
def files_generator(directory_path):
    file_path_generator = glob.iglob(f'{directory_path}/*.csv')
    return file_path_generator

file_path_generator = files_generator(directory_path)


def extract_region(file_name):
    # Extact the region name from the file name
    region_pattern = r"unformatted-(.*?)-mid2002"
    match = re.search(region_pattern, file_name)

    if match:
        region = match.group(1)

    return region

def create_region_generator(directory_path):
    file_path_generator = files_generator(directory_path)
    return (extract_region(file) for file in file_path_generator)
       
    
def load_pickle_or_csv(pickle_path: str, csv_path: str, yr: str) -> Tuple[pd.DataFrame, bool]:
    persistent_exists = os.path.isfile(pickle_path)
    if persistent_exists:
        with open(pickle_path, "rb") as pickle_file:
            df = pickle.load(pickle_file)
            logging.info("Pickle file loaded successfully.")
    else:
        # Read the CSV file into a dataframe
        year_data_col = f"Population_{yr}"
        cols_list = ["OA11CD", "Sex", "Age", "LAD11CD", year_data_col]
        df = pd.read_csv(csv_path, usecols=cols_list)
    return df, persistent_exists



        
def proc_each_region_for_all_years(file_path: str, df_dic: dict, year_gen: Generator[str, None, None], pickle_file_path: str) -> None:
    """Processes the file for each region for all years."""
        
    # Extract the region name from the file name
    region_pattern = r"unformatted-(.*?)-mid2002"
    match = re.search(region_pattern, file_path)

    if match:
        region = match.group(1)
        logging.info("Processing %s", region)

        region_for_all_years = {}
      

        for year in year_gen:
            logging.info("Processing %s", year)
                
            # # Load pickle if it exists or CSV if not
            df, _ = load_pickle_or_csv(pickle_file_path, file_path, year)
            
            # Check if there is a region key in the dict, if not initialize an empty dictionary for the region
            year_key = f"{year}_data"
            
            region_for_all_years[year] = df

        
        # we now have all years processed for one region, so save the dictionary to a pickle file
        pickle_file_path = f"data/population_estimates/interim/{region}_data.pickle"
        
        # Save the updated dictionary to the pickle file
        with open(pickle_file_path, "wb") as pickle_file:
            pickle.dump(region_for_all_years, pickle_file)
        
        # Delete dict of dfs to free up mem
        del region_for_all_years

def concat_all_regions_same_year(dfs_dict, regions, yr):
    regions = list(regions)
    logging.info("Concatenating %s for these regions %s", yr, ', '.join(regions))
    # Create a list of dataframes with dfs for each region for one year (yr)
    all_regions_in_year = [dfs_dict[f"{yr}_data"][region] for region in regions]
    # Concatenate that list to bring together all the regions for that year (yr)
    concated_df = pd.concat(all_regions_in_year)
    
    # we now have all years processed for one region, so save the dictionary to a pickle file
    feath_file_path = f"data/population_estimates/interim/EW_{yr}_data.feather"
    # TODO: Change "EW" to an arugment in the function, so can take "NI" etc
    
    # Save the updated dictionary to the pickle file
    di._pd_to_feather(concated_df, feath_file_path)
    
    # Delete df to save on memory
    del concated_df


def age_pop_one_year(df, sex_num, year):
    """Gets the population for each age in every OA in a region for one year.
        Makes the data go from long-narrow format to wide.
    
    Supply sex_num, 1 for male and 2 for female, None for both."""
    
    if sex_num:
        sex_mask = df.Sex==sex_num
        # get single sex df
        df = df[sex_mask]
    # drop sex column
    cols = ["OA11CD", "Age", "LAD11CD", f"Population_{year}"]
    df = df[cols]
    grouped_df = df.groupby(["OA11CD", "Age"]).sum().pivot_table(index="OA11CD", columns="Age", values="Population_2002")
    grouped_df["All Ages"] = grouped_df.sum(axis=1)
    
    return grouped_df


def all_regions_by_year_split_sex(all_regions_by_year, age_pop_one_year):
    """Creates the sex-disaggregated tables"""
    for year, year_df in all_regions_by_year():
        both_sexes_df = age_pop_one_year(df=year_df, sex_num=None, year=year)
        male_df = age_pop_one_year(df=year_df, sex_num=1, year=year)
        female_df = age_pop_one_year(df=year_df, sex_num=2, year=year)
    # Replace the data for the year in the all_regions_by_year dictionary with the new dataframes
        all_regions_by_year[year] = {
    f"Mid-{year} Persons" : both_sexes_df,
    f"Mid-{year} Males" : male_df,
    "Mid-{year} Females" : female_df    
    }
        
    return all_regions_by_year


def write_to_tabbed_excel(all_regions_by_year, output_path):
    """Writes the dataframes to a tabbed excel file trying to be as close as possible 
        to the data in the population estimates publised annually by ONS.
        
        It will output the data for each sex into separate tabs.
        
        Unlike the ONS data, it will have data for all regions in the same file."""
    
    sheets = all_regions_by_year
    
    cover = gpt.Cover(
    cover_label="Cover",
    title="A Workbook containing good practice tables",
    intro=["This is some introductory information", "And some more"],
    about=["Even more info about my data", "And a little more"],
    contact=["John Doe", "Tel: 345345345", "Email: [john.doe@snailmail.com](mailto:john.doe@snailmail.com)"],
    )
    
    gpt.write_workbook(
        filename=output_path,
        sheets=sheets,
        cover=cover)
    

if __name__ == "__main__":
    # Call all the generator functions
    year_gen = years_generator()
    files_gen = files_generator(directory_path)
    regs_gen = create_region_generator(directory_path)
    
    # Call proc_all_regions_by_year for each file in the directory
    for file_path in file_path_generator:
        """Each file contains data for one region for all sexes, all ages and all years"""
        proc_each_region_for_all_years(file_path=file_path,
                                df_dic=dfs_dict,
                                year_gen=years_generator(),
                                pickle_file_path="fake/path/to/pickle/file")
        
    
    # Now concatenate all the regions for a given year. Each year will be a separate dataframe
    # but all regions' data will be stacked into the df 
    for year in years_generator():
        regs_gen = create_region_generator(directory_path)
        concat_all_regions_same_year(dfs_dict, regions=regs_gen, yr=year)

    # Delete the dfs_dict to free up memory
    del dfs_dict
    
    # Get a dataframe for each sex for all regions by each year in wide format
    all_regions_by_year_dict = all_regions_by_year_split_sex(all_regions_by_year, age_pop_one_year)

    # Write the dataframes to a tabbed excel file
    write_to_tabbed_excel(all_regions_by_year_dict, output_path="test_output.xlsx")