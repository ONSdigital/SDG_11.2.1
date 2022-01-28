import pandas as pd
import numpy as np

def reshape_for_output(df, id_col="Age"):
    # 1) Transpose the df
    # 2) reset index 
    # 3) rename column from index to Age or other id column
    # 4) melt df with Age as ID vars, all the rest value vars
    # 5) Replace word "Total" with blanks, "variable" with "Series"
    # 6) Create "Unit Multiplier" map across "Unserved" to 
    df = df.T
    df = df.reset_index()
    df = df.rename(columns={"index":id_col})
    df = pd.melt(df, id_vars=id_col, value_vars=["Total", "Served", "Unserved", "Percentage served", "Percentage unserved"])
    df = df.replace({"Total":""})
    df["Unit Multiplier"] = np.where(df.variable.str.contains("Percentage"), "percent", "individual")
    # Rename the variables in the "variable" column
    df["variable"].replace(to_replace="Percentage served", value="Served")
    df["variable"].replace(to_replace="Percentage unserved", value="Unserved")
    return df