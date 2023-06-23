"""All functions related to data validation and cleaning."""

def uppercase_column_names(df):
    df.columns = df.columns.str.upper()
    df.rename(columns={'GEOMETRY': 'geometry'}, inplace=True)


def check_required_columns(dataframe, required_columns):
    missing_columns = []
    for column in required_columns:
        if column not in dataframe.columns:
            missing_columns.append(column)
    if missing_columns:
        raise ValueError(f"The following columns are missing: {missing_columns}")
