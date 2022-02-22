import pandas as pd
import numpy as np

def reshape_for_output(df, id_col, local_auth, id_rename=None):
    """ Reshapes the output of served_proportions_disagg to data team requirements.
    
    The steps the function goes through are as follows.
    1) Transpose the df
    2) Reset the index 
    3) Rename column from index to Age or other id column
    4) melt df with Age as ID vars, all the rest value vars
    5) Replace word "Total" with blanks, "variable" with "Series"
    6) Create "Unit Multiplier" map across from variable (percent or individual)
    7) Create "Unit Measure" and "Observation Status" columns
    7) Create the local auth col with this iteration's LA
    8) Rename column header of ID var
    9) re-order columns to match required output

    Args:
        df (_type_): _description_
        id_col (_type_): _description_
        local_auth (_type_): _description_
        id_rename (_type_, optional): _description_. Defaults to None.

    Returns:
        _type_: _description_
    """
    # Transpose the df
    df = df.T
    #Reset the index 
    df = df.reset_index()
    # Rename column from index to id_column, e.g "Age"
    df = df.rename(columns={"index":id_col})
    # Melt df with Age as ID vars, all the rest value vars
    df = pd.melt(df, id_vars=id_col, value_vars=["Total", "Served", "Unserved", "Percentage served", "Percentage unserved"])
    # Replace word "Total" with blanks
    df = df.replace({"Total":""})
    # Create "Unit Multiplier" map across from variable (percent or individual)
    df["Unit Multiplier"] = np.where(df.variable.str.contains("Percentage"), "percent", "individual")
    # Create "Unit Measure" and "Observation Status" columns
    df["Unit Measure"] = "Units"
    df["Observation Status"] = "Undefined"
    # Rename the variables in the "variable" column
    df["variable"].replace(to_replace="Percentage served", value="Served", inplace=True)
    df["variable"].replace(to_replace="Percentage unserved", value="Unserved", inplace=True)
    # Rename the "variable" col to "Series"
    df.rename(columns={"variable":"Series"},inplace=True)
    # Rename "value" to "Value" as required
    df.rename(columns={"value":"Value"},inplace=True)
    # Add and populate the "Local Authority" column
    df["Local Authority"]=local_auth
    df = df[[id_col,"Local Authority", "Series", "Observation Status", "Unit Multiplier", "Unit Measure", "Value"]]
    #  Rename column header of ID var
    if id_rename:
        df.rename(columns={id_col:id_rename}, inplace=True)
    return df

def reorder_final_df(df):
    df = df[["Year","Sex", "Age", "Disability Status", "Local Authority", "Urban/Rural", "Series", "Observation Status", "Unit Multiplier", "Unit Measure", "Value"]]
    return df