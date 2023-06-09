"""All functions related to data validation and cleaning."""
import re

def uppercase_column_names(df):
    df.columns = df.columns.str.upper()
    df.rename(columns={'GEOMETRY': 'geometry'}, inplace=True)


def check_required_columns(dataframe, required_columns):
    """
    Checks if the required columns are present in the dataframe.

    This function iterates over the required_columns list and checks if each column is present in the dataframe.
    If any of the required columns are missing, it raises a ValueError with a list of the missing columns.

    Parameters:
    dataframe (pandas.DataFrame): The dataframe to check.
    required_columns (list): A list of column names that are required.

    Raises:
    ValueError: If any of the required columns are missing from the dataframe.
    """
    missing_columns = []
    for column in required_columns:
        if column not in dataframe.columns:
            missing_columns.append(column)
    if missing_columns:
        raise ValueError(f"The following columns are missing: {missing_columns}")
