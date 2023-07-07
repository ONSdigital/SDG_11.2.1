# """This program is used to pre-process the population estimates archive data.
# The archived data is in a different format than the current data. It has many years's data in one file for each region.
# Also the data is a long format, so it needs to be pivoted to a wide format."""

import re
import pathlib as pl
from glob import glob
import duckdb
import uuid
from typing import List



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
    "Age": "INTEGER",
    "Sex": "INTEGER",
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


def create_connection(database_path):
    """Creates a connection to a DuckDB database and returns the connection object."""
    con = duckdb.connect(database_path)
    return con


def load_all_csvs(con, csv_folder, output_table_name):
    """Loads all the population csv files in a folder into a DuckDB database."""

    load_csv_query = f"""
    CREATE TABLE IF NOT EXISTS {output_table_name}
    AS SELECT *
    FROM read_csv_auto('{csv_folder}/*.csv', header=true, columns={column_types}, delim=',', auto_detect=false);
    """
    #
    #
    con.execute(load_csv_query)

    return output_table_name

def query_database(con, query):
    """Queries a DuckDB database and returns the name of a temporary table.

    This function takes a DuckDB connection object and a SQL query as input,
    and creates a temporary tcable with the results of the query. The function
    returns the *name* of the temporary table, which can be passed to the next function.
    """
    # Generate a unique name for the temporary table
    table_name = f"temp_{uuid.uuid4().hex}"

    # Create the temporary table and insert the query results
    con.execute(f"CREATE TEMPORARY TABLE {table_name} AS {query}")

    # Return the name of the temporary table
    return table_name

def query_database_as_view(con, query, view_name):
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

    if match:
        region = match.group(1)

    return region


def pivot_sex_tables(con, male_table: str, female_table: str, both_table: str, year_col: str):
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

    # Construct the SQL query to pivot the male sex-specific table
    male_query = f"""
        SELECT OA11CD, LAD11CD, {', '.join([f"SUM(CASE WHEN Age = {i} THEN {year_col} ELSE 0 END) AS Age_{i}" for i in range(0, 101)])}
        FROM {male_table}
        GROUP BY OA11CD, LAD11CD;"""

    # Construct the SQL query to pivot the female sex-specific table
    female_query = f"""
        SELECT OA11CD, LAD11CD, {', '.join([f"SUM(CASE WHEN Age = {i} THEN {year_col} ELSE 0 END) AS Age_{i}" for i in range(0, 101)])}
        FROM {female_table}
        GROUP BY OA11CD, LAD11CD;"""

    # Construct the SQL query to pivot the both sex-specific table
    both_query = f"""
        SELECT OA11CD, LAD11CD, {', '.join([f"SUM(CASE WHEN Age = {i} THEN {year_col} ELSE 0 END) AS Age_{i}" for i in range(0, 101)])}
        FROM {both_table}
        GROUP BY OA11CD, LAD11CD;"""

    # Execute the SQL queries and get dataframes for each sex-specific table
    male_df = query_database(con, male_query)
    female_df = query_database(con, female_query)
    both_df = query_database(con, both_query)

    return male_df, female_df, both_df


def age_pop_by_sex(con: duckdb.DuckDBPyConnection, table_name, year: int):
    """"Uses SQL to get the data for three sex groups and drops the sex column.

    Args:
        con: A DuckDB connection object.
        table_name: The name of the table which corresponds to one year of data
        year: An integer representing the year to query.
        sex_num:
    """

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
        SELECT OA11CD, Age, LAD11CD, Population_2002
        FROM {table_name};"""

    # Get a dataframe for sex == 1
    male_table = query_database(con, male_query)

    # Get a dataframe for sex == 2, Female
    female_table = query_database(con, female_query)

    # Get combined sexes dataframe
    both_table = query_database(con, both_query)

    return male_table, female_table, both_table



def create_output_folder(year: int) -> pl.Path:
    """Create the output folder for a given year if it doesn't exist."""
    output_folder = pl.Path(f"data/population_estimates/{year}")
    if not output_folder.exists():
        output_folder.mkdir(parents=True)
    return output_folder


def write_table_to_csv(con, *args, output_folder: pl.Path, year: int):

    for table_name in args:
        query = f"""
            COPY {table_name}
            TO 'data/population_estimates/{year}/pop_estimate_{year}.csv';"""
        
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
    for year in years:

        year_col=f"Population_{year}"


        # Get the three sex disaggregated tables
        both_sexes_table, male_table, female_table = age_pop_by_sex(con, table_name, year)

        # Pack the three names as a tuple
        all_three_tables = both_sexes_table, male_table, female_table

        # Pivot the dataframe to a wide format
        all_three_tables = pivot_sex_tables(con, *all_three_tables, year_col)

        # Create the folder for the year, and return its path
        output_folder = create_output_folder(year)

        # Write out the pivoted tables as CSV files
        write_table_to_csv(con, *all_three_tables,
                           output_folder=output_folder,
                           year=year)

    # Close the connection
    con.close()

if __name__ == "__main__":
    """This is executed when run from the command line!"""
    main()
