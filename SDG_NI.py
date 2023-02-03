# core imports
import os
import random
import time

# third party import
import yaml
import pandas as pd
import geopandas as gpd

# module imports
import geospatial_mods as gs
import data_ingest as di
import data_transform as dt
import data_output as do

# timings
start = time.time()

# get current working directory
CWD = os.getcwd()

# Load config
with open(os.path.join(CWD, "config.yaml")) as yamlfile:
    config = yaml.load(yamlfile, Loader=yaml.FullLoader)
    module = os.path.basename(__file__)
    print(f"Config loaded in {module}")

# Constants
pop_year = str(config["calculation_year"])
DATA_DIR = config["DATA_DIR"]
boundary_year = "2021"
DEFAULT_CRS = config["DEFAULT_CRS"]


#grabs northern ireland bus stops path
ni_bus_stops_path = os.path.join(CWD, "data", "stops", "NI", "bus_stops_ni.csv")

# reads in NI bus stop data as pandas df
ni_bus_stops = pd.read_csv(ni_bus_stops_path, index_col=0)

# assigns capacity type for bus stops as low
ni_bus_stops['capacity_type'] = 'low'

# gets the northern ireland train stops data path
ni_train_stops_path = os.path.join(
    CWD, "data", "stops", "NI", "train_stops_ni.csv")

# reads in the NI train stop data as pandas df
ni_train_stops = pd.read_csv(ni_train_stops_path, index_col=0)

# assigns capacity type for train stops as high
ni_train_stops['capacity_type'] = 'high'


# Join the two stops dataframes together
stops_df = ni_bus_stops.merge(
    ni_train_stops, on=['capacity_type', 'Latitude', 'Longitude'], how='outer')

stops_geo_df = di.geo_df_from_pd_df(pd_df=stops_df,
                                    geom_x='Longitude',
                                    geom_y='Latitude',
                                    crs='EPSG:4326')

# Convert latitude and longitude to easting and northing
stops_geo_df = dt.convert_east_north(stops_geo_df, 'Longitude', 'Latitude')

# Get usual population for Northern Ireland (Census 2011 data)
census_ni_df = pd.read_csv(os.path.join(CWD, "data", "KS101NI.csv"))

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
oa_to_sa_lookup_path = os.path.join(CWD, "data", "oa_la_mapping",
                                    "NI",
                                    "OA_to_SA.csv")


# reads in the OA to SA lookupfile
sa_to_la = pd.read_csv(oa_to_sa_lookup_path,
                       usecols=["COA2001_1", "SA2011"])

# getting the coordinates for all LA's
uk_la_file = di.geo_df_from_geospatialfile(path_to_file=uk_la_path)
ni_la_file = uk_la_file[uk_la_file["LAD21CD"].str[0].isin(['N'])]

# Get population weighted centroids into a dataframe
ni_pop_wtd_centr_df = (di.geo_df_from_geospatialfile
                       (os.path.join
                        (DATA_DIR,
                         'pop_weighted_centroids',
                         "NI",
                         "NI_PWC_BNG.shp")))

pwc_with_lookup = pd.merge(left=sa_to_la,
                           right=ni_pop_wtd_centr_df,
                           left_on=sa_to_la['COA2001_1'],
                           right_on='OA_CODE',
                           how='left')

# get weighted centroids and merge with population
pwc_with_pop = pd.merge(left=census_ni_df,
                        right=pwc_with_lookup,
                        left_on=census_ni_df["SA Code"],
                        right_on="SA2011",
                        how="left")

# Drops OA codes as not required, Coords as geographical, and geometry as
# this is for output areas not small areas
pwc_with_pop.drop(["SA Code", "OA_CODE", "COA2001_1"], axis=1, inplace=True)

# SA to LA lookup
sa_to_la_lookup_path = os.path.join(CWD, "data", "oa_la_mapping",
                                    "NI",
                                    "11DC_Lookup_1_0.csv")

# reads in the OA to LA lookupfile
sa_to_la = pd.read_csv(sa_to_la_lookup_path)

# merges the pwc with it's corresponding LA
pwc_with_pop_with_la = pd.merge(left=pwc_with_pop,
                                right=sa_to_la,
                                left_on="SA2011",
                                right_on="SA2011",
                                how="left")

# read in urban/rural classification
urb_rur_path = os.path.join(CWD, "data", "urban_rural", "NI",
                            "NI_urb_rur_class.csv")

urb_rur = di.read_urb_rur_ni(urb_rur_path)

pwc_with_pop_with_la = pd.merge(left=pwc_with_pop_with_la,
                                right=urb_rur,
                                left_on="SA2011",
                                right_on = 'SA2011_Code',
                                how='left')

# Rename columns to fit functions below
pwc_with_pop_with_la.rename(
    columns={
        'SA2011': 'OA11CD',
        "All usual residents": "pop_count"},
    inplace=True)

# Read disability data for disaggregations later
disability_df = pd.read_csv(os.path.join(CWD,
                                         "data", "disability_status",
                                         "qs303_ni.csv"), skiprows=5)

# Remove the first column because it's a repeat of SA code
disability_df.drop(['SA'], axis=1, inplace=True)

# the col headers are database unfriendly. Defining their replacement names
replacements = {
    "SA Code": 'OA11CD',
    "Long-term health problem or disability: Day-to-day activities limited a lot": "disab_ltd_lot",
    "Long-term health problem or disability: Day-to-day activities limited a little": "disab_ltd_little",
    'Long-term health problem or disability: Day-to-day activities not limited': "disab_not_ltd"}

# renaming the dodgy col names with their replacements
disability_df.rename(columns=replacements, inplace=True)

# defining age data path
age_path = os.path.join(CWD,"data","census-2011-qs103ni.xlsx")

# reading in age data
age_df = di.read_ni_age_df(age_path)
                       
# gets northern ireland age list
age_lst = config['ni_age_lst']      

# slices df to just age cols
age_df_sliced = dt.slice_age_df(age_df, age_lst)

# Create a list of tuples of the start and finish indexes for the age bins
age_bins = dt.get_col_bins(age_lst)

# get the ages in the age_df binned, and drop the original columns
age_df = dt.bin_pop_ages(age_df_sliced, age_bins, age_lst)

# merge ages back onto dataframe
pwc_with_pop_with_la = pd.merge(pwc_with_pop_with_la, age_df, left_on="OA11CD", right_index=True)

# Unique list of LA's to iterate through
list_local_auth = ni_la_file["LAD21NM"].unique()
random_la = random.choice(list_local_auth)
ni_auth = [random_la]

total_df_dict = {}
disab_df_dict = {}
age_df_dict = {}
sex_df_dict = {}

for local_auth in ni_auth:
    print(f"Processing: {local_auth}")

    # Get a polygon of la based on the Location Code
    la_poly = (gs.get_polygons_of_loccode(
        geo_df=ni_la_file,
        dissolveby="LAD21NM",
        search=local_auth))

    # Creating a Geo Dataframe of only stops in la
    la_stops_geo_df = (gs.find_points_in_poly
                       (geo_df=stops_geo_df,
                        polygon_obj=la_poly))

    # buffer around the stops
    la_stops_geo_df = gs.buffer_points(la_stops_geo_df)

    # filter only by current la
    only_la_pwc_with_pop = gpd.GeoDataFrame(pwc_with_pop_with_la[pwc_with_pop_with_la["LGD2014NAME"] == local_auth],
                                            geometry='geometry', crs='EPSG:27700')
    
    # Disability disaggregation

    only_la_pwc_with_pop = dt.disab_disagg(disability_df, only_la_pwc_with_pop)

    # find all the pop centroids which are in the la_stops_geo_df
    pop_in_poly_df = gs.find_points_in_poly(
        only_la_pwc_with_pop, la_stops_geo_df)

    # Deduplicate the df as OA appear multiple times
    pop_in_poly_df = pop_in_poly_df.drop_duplicates(subset="OA11CD")

    # all the figures we need
    served = pop_in_poly_df["pop_count"].astype(int).sum()
    full_pop = only_la_pwc_with_pop["pop_count"].astype(int).sum()
    not_served = full_pop - served
    pct_not_served = "{:.2f}".format(not_served / full_pop * 100)
    pct_served = "{:.2f}".format(served / full_pop * 100)

    print(f"""The number of people who are served by public transport is {served}.\n
            The full population of {local_auth} is calculated as {full_pop}
            While the number of people who are not served is {not_served}""")

    # putting results into dataframe
    la_results_df = pd.DataFrame({"All_pop": [full_pop],
                                  "Served": [served],
                                  "Unserved": [not_served],
                                  "Percentage served": [pct_served],
                                  "Percentage unserved": [pct_not_served]})

    # Re-orienting the df to what's accepted by the reshaper and renaming col
    la_results_df = la_results_df.T.rename(columns={0: "Total"})

    # Feeding the la_results_df to the reshaper
    la_results_df_out = do.reshape_for_output(la_results_df,
                                              id_col="Total",
                                              local_auth=local_auth)

    # Finally for the local authority totals the id_col can be dropped
    # That's because the disaggregations each have their own column,
    # but "Total" is not a disaggregation so doesn't have a column.
    # It will simply show up as blanks (i.e. Total) in all disagg columns
    la_results_df_out.drop("Total", axis=1, inplace=True)

    # Output this iteration's df to the dict
    total_df_dict[local_auth] = la_results_df_out

    # Disability disaggregation - get disability results in disab_df_dict
    disab_df_dict = dt.disab_dict(only_la_pwc_with_pop, pop_in_poly_df, disab_df_dict, local_auth)

    # Sex disaggregation
    # # # renaming Scotland sex col names with their replacements
    replacements = {"Males": "male",
                    "Females": "female"}
    only_la_pwc_with_pop.rename(columns=replacements, inplace=True)
    pop_in_poly_df.rename(columns=replacements, inplace=True)
    # # Calculating those served and not served by sex
    sex_cols = ['male', 'female']

    sex_servd_df = dt.served_proportions_disagg(pop_df=only_la_pwc_with_pop,
                                                pop_in_poly_df=pop_in_poly_df,
                                                cols_lst=sex_cols)

    # Feeding the results to the reshaper
    sex_servd_df_out = do.reshape_for_output(sex_servd_df,
                                             id_col="Sex",
                                             local_auth=local_auth)

    # Output this iteration's sex df to the dict
    sex_df_dict[local_auth] = sex_servd_df_out


    ## Age disaggregation
    age_bins = ['0-4', '5-9', '10-14', '15-19', '20-24',
                 '25-29', '30-34', '35-39', '40-44', '45-49', '50-54',
                 '55-59', '60-64', '65-69', '70-74', '75-79',
                 '80-84', '85-89', '90+']
    
    age_servd_df = dt.served_proportions_disagg(pop_df=only_la_pwc_with_pop,
                                                pop_in_poly_df=pop_in_poly_df,
                                                cols_lst=age_bins)
    
    # Feeding the results to the reshaper
    age_servd_df_out = do.reshape_for_output(age_servd_df,
                                             id_col="Age",
                                             local_auth=local_auth)

    # Output this local auth's age df to the dict
    age_df_dict[local_auth] = age_servd_df_out

# every single LA
all_la = pd.concat(total_df_dict.values())
sex_all_la = pd.concat(sex_df_dict.values())
disab_all_la = pd.concat(disab_df_dict.values())
all_age = pd.concat(age_df_dict.values())

# Stacking the dataframes
all_results_dfs = [all_la, disab_all_la,all_age, sex_all_la]
final_result = pd.concat(all_results_dfs)
final_result["Year"] = pop_year

# output to CSV
final_result.to_csv("NI_results.csv", index=False)


# end time
end = time.time()

print(f"This took {(end-start)} seconds to run")
