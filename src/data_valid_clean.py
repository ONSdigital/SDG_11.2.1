"""All functions related to data validation and cleaning."""


def uppercase_column_names(df):
    df.columns = df.columns.str.upper()
    df.rename(columns={'GEOMETRY': 'geometry'}, inplace=True)


def check_df_col_names(df, correct_cols, regex_cols=None):
    """Check that the column names of a dataframe are correct.
        Will accept a list of column names in any order.
        Any column can be a regex pattern.

    Args:
        df (pd.DataFrame): Dataframe to check.
        correct_cols (list): List of correct column names.

    Returns:
        bool: True if the column names are correct, False otherwise.
    """
    
    if regex_cols:
        for col in regex_cols:
            
    
    return list(df.columns) == correct_cols