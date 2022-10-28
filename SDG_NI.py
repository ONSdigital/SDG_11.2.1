# core imports
import os

# third party import 
import yaml
import pandas as pd

# module imports
import data_ingest as di

# get current working directory
CWD = os.getcwd()

# Load config
with open(os.path.join(CWD, "config.yaml")) as yamlfile:
    config = yaml.load(yamlfile, Loader=yaml.FullLoader)
    module = os.path.basename(__file__)
    print(f"Config loaded in {module}")

# Years
# Getting the year for population data
pop_year = str(config["calculation_year"])
DATA_DIR = config["DATA_DIR"]
boundary_year="2021"

# Load config
with open(os.path.join(CWD, "config.yaml")) as yamlfile:
    config = yaml.load(yamlfile, Loader=yaml.FullLoader)
    module = os.path.basename(__file__)
    print(f"Config loaded in {module}")

# gets the northern ireland bus stops data from the api
ni_bus_stop_url = config["NI_bus_stops_data"]
output_ni_bus_csv = os.path.join(CWD,"data","Stops","NI","bus_stops_ni.csv")

# reads in the NI bus stop data as geo df and saves bus data if it has not been saved
ni_bus_stops = di.read_ni_stops(ni_bus_stop_url, output_ni_bus_csv)

# gets the northern ireland train stops data from the api
ni_train_stop_url = config["NI_train_stops_data"]
output_ni_train_csv = os.path.join(CWD,"data","Stops","NI","train_stops_ni.csv")

# reads in the NI train  stop data as geo df and saves train data if it has not been saved
ni_train_stops = di.read_ni_stops(ni_train_stop_url, output_ni_train_csv)

# Get usual population for Northern Ireland (Census 2011 data)
whole_NI_df = pd.read_csv(os.path.join(CWD, "data", "KS101NI.csv"),
                             header=2)
# Only use columns that we need
cols_NI_df = ["SA Code", "All usual residents","Usual residents: Males","Usual residents: Females"]
census_ni_df = whole_NI_df[cols_NI_df]

# Read in mid-year population estimates for Northern Ireland
pop_files = pd.read_csv(os.path.join(CWD, 
                                     "data", "population_estimates", 
                                     "SAPE20-SA-Totals.csv"),
                        header=7)

# Filter to small area code and population year columns only
estimate_cols = ["Area_Code", pop_year]
estimate_pop_NI = pop_files[estimate_cols]

# getting path for .shp file for LA's
uk_la_path = di.get_shp_abs_path(dir=os.path.join(os.getcwd(),
                                                   "data",
                                                   "LA_shp",
                                                   boundary_year))

# Need OA to SA lookup so we can map to SA for pop weighted centroids
oa_to_sa_lookup_path = os.path.join(CWD, "data","oa_la_mapping",
                                    "NI",
                                    "Look-up Tables_0.csv")


# reads in the OA to SA lookupfile 
sa_to_la = pd.read_csv(oa_to_sa_lookup_path,
                        usecols=["COA2001","SA2011"])

# getting the coordinates for all LA's
uk_la_file = di.geo_df_from_geospatialfile(path_to_file=uk_la_path)
ni_la_file = uk_la_file[uk_la_file["LAD21CD"].str[0].isin(['N'])]

# Get population weighted centroids into a dataframe
ni_pop_wtd_centr_df = (di.geo_df_from_geospatialfile
                       (os.path.join
                        (DATA_DIR,
                         'pop_weighted_centroids',
                         "NI",
                         "2011",
                         "OA_ni.shp")))

pwc_with_lookup = pd.merge(left=sa_to_la,
                            right=ni_pop_wtd_centr_df,
                            left_on=sa_to_la['COA2001'],
                            right_on='OA_CODE',
                            how='left')

# get weighted centroids and merge with population
pwc_with_pop = pd.merge(left=census_ni_df,
                             right=pwc_with_lookup,
                             left_on=census_ni_df["SA Code"],
                             right_on="SA2011",
                             how="left")

# Drops SA code as repeat of COA2011
pwc_with_pop.drop("SA Code", axis=1, inplace=True)

# SA to LA lookup
sa_to_la_lookup_path = os.path.join(CWD, "data","oa_la_mapping",
                                    "NI",
                                    "11DC_Lookup_1_0.csv")



# reads in the OA to LA lookupfile 
sa_to_la = pd.read_csv(sa_to_la_lookup_path,
                        usecols=["SA2011","LGD2014"])

# merges the pwc with it's corresponding LA
pwc_with_pop_with_la = pd.merge(left=pwc_with_pop,
                                right=sa_to_la,
                                left_on="SA2011",
                                right_on="SA2011",
                                how="left")

# Rename columns to fit functions below
pwc_with_pop_with_la.rename(columns={'SA2011':'OA11CD', "All usual residents":"pop_count"}, 
                            inplace=True)




