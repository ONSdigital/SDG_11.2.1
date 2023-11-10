# """This program is used to pre-process the population estimates archive data.
# The archived data is in a different format than the current data. It has many years's data in one file for each region.
# Also the data is a long format, so it needs to be pivoted to a wide format."""

import re
import pathlib as pl
from glob import glob
import duckdb
import uuid
from typing import List, Dict
from duckdb import DuckDBPyConnection
import logging


# Define the input and output file paths
input_folder = "data/population_estimates/2002-2012"


# Set up logging
logging.basicConfig(level=logging.DEBUG, format="%(asctime)s %(levelname)s %(message)s")

# Create logger
logger = logging.getLogger(__name__)

db_file_path = "data/population_estimates/2002-2012/pop_est_2002-2012.db"

# Define the input and output file paths
input_folder = "data/population_estimates/2002-2012"

# Define the years to process
years = list(range(2002, 2013))

# Define an empty dictionary to store the dataframes for each year
year_data = {}

# Define the column name for the year
year_cols = [f"Population_{year}" for year in years]


column_types = {
    "OA11CD": "TEXT",
    "Sex": "INTEGER",
    "Age": "INTEGER",
    "LAD11CD": "TEXT",
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


def create_connection(database_path: str) -> DuckDBPyConnection:
    """Creates a connection to a DuckDB database and returns the connection object."""
    con = duckdb.connect(database_path)
    return con


def load_all_csvs(con, csv_folder, output_table_name):
    """Loads all the population csv files in a folder into a DuckDB database."""

    logger.info(f"Loading all csv files")
    
    load_csv_query = f"""
    CREATE TABLE IF NOT EXISTS {output_table_name}
    AS SELECT *
    FROM read_csv_auto('{csv_folder}/*.csv', header=true, columns={column_types}, delim=',', auto_detect=false);
    """
    # This code currently gives an empty dataframe
    #
    con.execute(load_csv_query)

    logger.info(f"Finished loading all csv files in {csv_folder} into {output_table_name}")
    
    return output_table_name

def query_database(con, query, year=None):
    """Queries a DuckDB database and returns the name of a temporary table.

    This function takes a DuckDB connection object and a SQL query as input,
    and creates a temporary tcable with the results of the query. The function
    returns the *name* of the temporary table, which can be passed to the next function.
    """
    # Generate a unique name for the temporary table
    table_name = f"table_{uuid.uuid4().hex}_{year}"

    # Create the temporary table and insert the query results
    con.execute(f"CREATE TEMPORARY TABLE {table_name} AS {query}")

    logger.info(f"Created temporary table. Returning table name as {table_name}")
    
    # Return the name of the temporary table
    return table_name

def query_database_as_view(con: DuckDBPyConnection, query: str, view_name: str) -> None:
    """Queries a DuckDB database and creates or updates a view with the results.

    This function takes a DuckDB connection object, a SQL query, and a view name as input,
    and creates or updates a view with the results of the query. The function returns the view object,
    which can be used in subsequent queries.
    """
    # Create or update the view with the results of the query
    con.execute(f"CREATE OR REPLACE VIEW {view_name} AS {query};")

    # Return the updated view object
    return con.view(view_name)

def extract_region(file_name):
    # Extact the region name from the file name
    region_pattern = r"unformatted-(.*?)-mid2002"
    match = re.search(region_pattern, file_name)

# Define an empty dictionary to store the dataframes for each year
year_data = {}

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

def pivot_sex_tables(con, tables_dict: Dict[str, str], year: str):
    """Pivots the sex-specific tables using SQL.

    Args:
        con: A DuckDB connection object.
        male_table: A string representing the name of the male sex-specific table.
        female_table: A string representing the name of the female sex-specific table.
        both_table: A string representing the name of the both sex-specific table.
        year_col: A string representing the name of the column containing the population data.

    Returns:
        Three dataframes representing the pivoted male, female, and both sex-specific tables.
    """
    
    year_col=f"Population_{year}"
    
    logger.info(f"Starting the pivot process. Year column is: {year_col}")
     
    # Construct the SQL query to pivot the male sex-specific table
    male_query = f"""PIVOT {tables_dict['male']} ON Age USING SUM({year_col}) GROUP BY OA11CD;
    """
 
    # Construct the SQL query to pivot the female sex-specific table
    female_query = f"""PIVOT {tables_dict['female']} ON Age USING SUM({year_col}) GROUP BY OA11CD;
    """

    # Construct the SQL query to pivot the both sex-specific table
    both_query = f"""PIVOT {tables_dict['both']} ON Age USING SUM({year_col}) GROUP BY OA11CD;
    """

    # Execute the SQL queries and get dataframes for each sex-specific table
    logger.info("Executing SQL queries to pivot male table")
    male_table = query_database(con, male_query, year)
    logger.info("Executing SQL queries to pivot female table")
    female_table = query_database(con, female_query, year)
    logger.info("Executing SQL query to pivot table for both sexes")
    both_sexes_table = query_database(con, both_query, year)
            
    return {"both": both_sexes_table, "male": male_table, "female": female_table}


def age_pop_by_sex(con: duckdb.DuckDBPyConnection, table_name, year: int):
    """"Uses SQL to get the data for three sex groups and drops the sex column.

    Args:
        con: A DuckDB connection object.
        table_name: The name of the table which corresponds to one year of data
        year: An integer representing the year to query.
        sex_num:
    """
    # Generate a unique name for the temporary table
    # table_name = f"temp_{uuid.uuid4().hex}"

    # Construct the SQL query for the male sex group
    male_query = f"""
        SELECT OA11CD, Age, LAD11CD, Population_{year}
        FROM {table_name}
        WHERE Sex = 1;"""

    # Construct the SQL query for the female sex group
    female_query = f"""
        SELECT OA11CD, Age, LAD11CD, Population_{year}
        FROM {table_name}
        WHERE Sex = 2;"""
    # Construct the SQL query for both sex groups
    both_query = f"""
        SELECT OA11CD, Age, LAD11CD, Population_{year}
        FROM {table_name};"""

    # Get a dataframe for sex == 1
    logger.info("Executing query for male table")
    male_table = query_database(con, male_query, year)

    # Get a dataframe for sex == 2, Female
    logger.info("Executing query for female table")
    female_table = query_database(con, female_query, year)

    # Get combined sexes dataframe
    logger.info("Executing query for table for both sexes")
    both_table = query_database(con, both_query, year)

    logger.info("Finished executing queries. Returning three tables")
    return male_table, female_table, both_table


    # Get combined sexes dataframe
    both_table = query_database(con, both_query)
    
    return male_table, female_table, both_table



def create_output_folder(year: int) -> pl.Path:
    """Create the output folder for a given year if it doesn't exist."""
    output_folder = pl.Path(f"data/population_estimates/{year}")
    if not output_folder.exists():
        logger.info(f"Folder {output_folder} does not exist. Creating it now.")
        output_folder.mkdir(parents=True)
    return output_folder


def write_table_to_csv(con: DuckDBPyConnection, *args: str, output_folder: pl.Path, year: int) -> None:
    """
    Writes the specified tables in a DuckDB database to CSV files.

    This function takes a DuckDB connection object, a variable number of table names, an output folder path, 
    and a year as input. For each table name, it executes a COPY command to write the table to a CSV file 
    in the specified output folder. The CSV file is named 'pop_estimate_{year}.csv', where {year} is the 
    specified year. The function does not return a value.

    Parameters:
    con (DuckDBPyConnection): The DuckDB connection object.
    *args (str): The table names to write to CSV files.
    output_folder (Path): The path to the output folder.
    year (int): The year to include in the CSV file names.

    Returns:
    None
    """
    for table_name in args:
        query = f"""
            COPY {args[table_name]}
            TO '{output_folder}/pop_estimate_{table_name}_{year}.csv'
            (FORMAT CSV, DELIMITER ',', HEADER);"""
        
        query_database(con, query)
    
    return None

def main():
    """This is executed when run from the command line."""
    # Create connection
    con = create_connection(db_file_path)

    # Run query to load all the csv data in one go and create the table
    table_name = load_all_csvs(con, input_folder, "all_pop_estimates")


    # For each of those years, load the data for all regions into a temp table
    # and return the name of the temp table
    temp_table_names = []
    logger.info("Starting to load data for each year into a temp table")
    for year in years:
        logger.info(f"Extracting data for year {year}")

        year_col=f"Population_{year}"

        # Get the three sex disaggregated tables
        both_sexes_table, male_table, female_table = age_pop_by_sex(con, table_name, year)

        # Pack the three names as a tuple
        all_three_tables = {"both": both_sexes_table, "male": male_table, "female": female_table}

        # Pivot the dataframe to a wide format
        all_three_tables = pivot_sex_tables(con, all_three_tables, year)

        # Create the folder for the year, and return its path
        output_folder = create_output_folder(year)

        # Write out the pivoted tables as CSV files
        write_table_to_csv(con, all_three_tables,
                           output_folder=output_folder,
                           year=year)
    # drop the all pop estimates table so it can be run again
    con.execute(f"""DROP TABLE all_pop_estimates;""")

    # Close the connection
    con.close()

if __name__ == "__main__":
    """This is executed when run from the command line!"""
    main()
