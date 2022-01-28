from jinja2 import Undefined
import pandas as pd
import numpy as np

def reshape_for_output(df, id_col, local_auth, id_rename=None):
    # 1) Transpose the df
    # 2) reset index 
    # 3) rename column from index to Age or other id column
    # 4) melt df with Age as ID vars, all the rest value vars
    # 5) Replace word "Total" with blanks, "variable" with "Series"
    # 6) Create "Unit Multiplier" map across from variable (percent or individual)
    # 7) Create the local auth col with this iteration's LA
    # 8) rename column header of ID var
    # 9) re-order columns to match required output

    df = df.T
    df = df.reset_index()
    df = df.rename(columns={"index":id_col})
    df = pd.melt(df, id_vars=id_col, value_vars=["Total", "Served", "Unserved", "Percentage served", "Percentage unserved"])
    df = df.replace({"Total":""})
    df["Unit Multiplier"] = np.where(df.variable.str.contains("Percentage"), "percent", "individual")
    df["Unit Measure"] = "Units"
    df["Observation Status"] = "Undefined"
    # Rename the variables in the "variable" column
    df["variable"].replace(to_replace="Percentage served", value="Served", inplace=True)
    df["variable"].replace(to_replace="Percentage unserved", value="Unserved", inplace=True)
    # Rename the variable col to "Series"
    df.rename(columns={"variable":"Series"},inplace=True)
    # Rename "value" to "Value" as required
    df.rename(columns={"value":"Value"},inplace=True)
    # Add and populate the "Local Authority" column
    df["Local Authority"]=local_auth
    df = df[id_col,"Local Authority", "Series", "Observation Status", "Unit Multiplier","Unit Measure",]
    if id_rename:
        df.rename(columns={id_col:id_rename}, inplace=True)
    
    return df