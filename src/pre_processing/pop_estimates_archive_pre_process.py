"""This program is used to pre-process the population estimates archive data. 
The archived data is in a different format than the current data. It has many years's data in one file for each region.
Also the data is a long format, so it needs to be pivoted to a wide format."""

import os
import pandas as pd
import glob
import re
import pickle


# Define the directory path where the CSV files are located
directory_path = '/home/james/programming_projects/work/SDG_11.2.1/data/population_estimates/2002-2012'


# Initialize empty dictionaries to store the dataframes
all_regions_by_year = {}
all_ages_df = {}
male_df = {}
female_df = {}

# Create a generator of years
def years_generator():
    return map(str, list(range(2002, 2013)))

# Get a generator of all CSV files in the directory
file_path_generator = glob.iglob(f'{directory_path}/*.csv')


def extract_region(file_name):
    # Extact the region name from the file name
    region_pattern = r"unformatted-(.*?)-mid2002"
    match = re.search(region_pattern, file_name)

    if match:
        region = match.group(1)

    return region

regions_generator = (extract_region(file) for file in file_path_generator)
    
    
def load_pickle_file(pickle_path, csv_path):
    persistent_exists = os.path.isfile(pickle_path)
    if persistent_exists:
        with open(pickle_path, "rb") as pickle_file:
            df = pickle.load(pickle_file)
            print("Pickle file loaded successfully.")
    else:
        # Read the CSV file into a dataframe
        df = pd.read_csv(csv_path)
    return df, persistent_exists


dfs_dict = {}
        
def proc_all_regions_by_year(file_path, df_dic, year_gen, pickle_file_path):
    """Processes the file for each region for a given year."""
    
    # Extract the file name from the file path
    file_name = os.path.basename(file_path)
        
    # Extract the region name from the file name
    region_pattern = r"unformatted-(.*?)-mid2002"
    match = re.search(region_pattern, file_path)

    if match:
        region = match.group(1)
        print(f"Processing {region}")
        
        # # Load pickle if it exists or CSV if not
        df, _ = load_pickle_file(pickle_file_path, file_path)

        for year in year_gen:
            print(f"Processing {year}")
            year_data_col = f"Population_{year}"
                
            # Subset the dataframe to get the data for the year
            cols = ["OA11CD", "Sex", "Age", "LAD11CD", year_data_col]
            year_df = df[cols]
            
            # Check if there is a region key in the dict, if not initialize an empty dictionary for the region
            year_key = f"{year}_data"
            
            if year_key not in df_dic:
                df_dic.setdefault(year_key, {})

                # Add the data for the year to the dfs_dict[region] dictionary
                df_dic[year_key].update({region: year_df})
            else:
                print(f"{year_key} already in df_dic.")

for file_path in file_path_generator:
    proc_all_regions_by_year(file_path=file_path,
                             df_dic=dfs_dict,
                             year_gen=years_generator(),
                             pickle_file_path="fake/path/to/pickle/file")
    


def concat_all_regions_same_year(dfs_dict, regions, year):
   all_regions_in_year = [dfs_dict[f"{year}_data"][region] for region in regions]
   concated_df = pd.concat(all_regions_in_year)
   return concated_df


def age_pop_one_year(df, sex_num, year):
    """Gets the population for each age in every OA in a region for one year.capitalize
    
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

# Now concatenate all the regions for a given year
for year in years_generator():
    all_regions_by_year[year] = concat_all_regions_same_year(dfs_dict, regions_generator, year)

del dfs_dict


for year, year_df in all_regions_by_year():
    both_sexes_df = age_pop_one_year(df=year_df, sex_num=None, year=year)
    male_df = age_pop_one_year(df=year_df, sex_num=1, year=year)
    female_df = age_pop_one_year(df=year_df, sex_num=2, year=year)
    # Replace the data for the year in the all_regions_by_year dictionary with the new dataframes
    all_regions_by_year[year] = {
    "both_sexes" : both_sexes_df,
    "male" : male_df,
    "female" : female_df    
    }
    