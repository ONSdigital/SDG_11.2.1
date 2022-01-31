# Core imports
import os


# Third party imports
import geopandas as gpd
import pandas as pd
import yaml
import gptables as gpt

# Module imports
import geospatial_mods as gs
import data_ingest as di
import data_transform as dt

# get current working directory
CWD = os.getcwd()
# TODO: find out best practice on CWD

# Load config
with open(os.path.join(CWD, "config.yaml")) as yamlfile:
    config = yaml.load(yamlfile, Loader=yaml.FullLoader)
    print("Config loaded")

# Constants
DEFAULT_CRS = config["DEFAULT_CRS"]
DATA_DIR = config["DATA_DIR"]
EXT_ORDER = config['EXT_ORDER']

# define url for zip download
NAPT_ZIP_LINK = config["NAPT_ZIP_LINK"]

# Define the columns wanted from Naptan
COLS = list(config["NAPTAN_TYPES"].keys())
NAPTAN_DTYPES = config["NAPTAN_TYPES"]
# Get the pandas dataframe for the stops data
stops_df = di.any_to_pd(file_nm="Stops",
                        zip_link=NAPT_ZIP_LINK,
                        ext_order=EXT_ORDER,
                        dtypes=NAPTAN_DTYPES)

stops_geo_df = (di.geo_df_from_pd_df(pd_df=stops_df,
                                     geom_x='Easting',
                                     geom_y='Northing',
                                     crs=DEFAULT_CRS))


# getting the coordinates for all LA's
uk_la_file_conf = config['uk_la_shp_file']
full_path_uk_la=os.path.join(os.getcwd(), "data", "LA_shp", uk_la_file_conf)
uk_la_file=di.geo_df_from_geospatialfile(path_to_file=full_path_uk_la)

# Getting the west midlands population estimates for 2019
pop_year = config["calculation_year"]

# Get list of all pop_estimate files for target year
pop_files = os.listdir(os.path.join(
                                    "data/population_estimates",
                                    str(pop_year)
                                    )
                       )

# Get the population data for the whole nation for the specified year
whole_nation_pop_df = di.get_whole_nation_pop_df(pop_files, pop_year)

# Get population weighted centroids into a dataframe
uk_pop_wtd_centr_df = (di.geo_df_from_geospatialfile
                       (os.path.join
                        (DATA_DIR,
                         'pop_weighted_centroids',
                         '2011')))

# Get output area boundaries
# OA_df = pd.read_csv(config["OA_boundaries_csv"])

# Links were changed at the source site which made the script fail. 
# Manually downloading the csv for now
OA_boundaries_df = pd.read_csv(
                               os.path.join("data",
                                            "Output_Areas__December_2011__Boundaries_EW_BGC.csv"))

# Merge with uk population df
uk_pop_wtd_centr_df = uk_pop_wtd_centr_df.merge(OA_boundaries_df, on="OA11CD", how='left')

# Clean after merge
uk_pop_wtd_centr_df.drop('OBJECTID_y', axis=1, inplace=True)
uk_pop_wtd_centr_df.rename({'OBJECTID_x': 'OBJECTID'}, inplace=True)

# Getting the urban-rural classification by OA for England and Wales
Urb_Rur_ZIP_LINK = config["Urb_Rur_ZIP_LINK"]
URB_RUR_TYPES = config["URB_RUR_TYPES"]

# Make a df of the urban-rural classification
urb_rur_df = (di.any_to_pd("RUC11_OA11_EW",
                           Urb_Rur_ZIP_LINK,
                           ['csv'],
                           URB_RUR_TYPES))

# These are the codes (RUC11CD) mapping to rural and urban descriptions (RUC11)
# I could make this more succinct, but leaving here
# for clarity and maintainability
urban_dictionary = {'A1': 'Urban major conurbation',
                    'C1': 'Urban city and town',
                    'B1': 'Urban minor conurbation',
                    'C2': 'Urban city and town in a sparse setting'}

# mapping to a simple urban or rural classification
urb_rur_df["urb_rur_class"] = (urb_rur_df.RUC11CD.map
                               (lambda x: "urban"
                                if x in urban_dictionary.keys()
                                else "rural"))

# filter the df. We only want OA11CD and an urban/rurual classification
urb_rur_df = urb_rur_df[['OA11CD', 'urb_rur_class']]

# joining urban rural classification onto the pop df
uk_pop_wtd_centr_df = (uk_pop_wtd_centr_df.merge
                       (urb_rur_df,
                        on="OA11CD",
                        how='left'))

# Joining the population dataframe to the centroids dataframe,
whole_nation_pop_df = whole_nation_pop_df.join(
    other=uk_pop_wtd_centr_df.set_index('OA11CD'), on='OA11CD', how='left')

# Map OA codes to Local Authority Names
LA_df = pd.read_csv("data/Output_Area_to_Lower_Layer_Super_Output_Area_to_Middle_Layer_Super_Output_Area_to_Local_Authority_District__December_2020__Lookup_in_England_and_Wales.csv", usecols=["OA11CD", "LAD20NM"])
whole_nation_pop_df = pd.merge(whole_nation_pop_df, LA_df, how="left", on="OA11CD")


# All of England LA areas
la_list_path="data/la_lookup_england_2019.csv"
local_authority_list=pd.read_csv(la_list_path)
list_local_auth=local_authority_list["LAD19NM"].unique()

output_df_list=[]

list_local_auth=["North Devon"]


# define output dicts to capture dfs
total_df_dict={}
sex_df_dict={}
urb_df_dict={}
rur_df_dict={}
disab_df_dict={}
age_df_dict={}

for local_auth in list_local_auth:
    print(local_auth)
    # Get a polygon of la based on the Location Code
    la_poly = (gs.get_polygons_of_loccode(
                            geo_df=uk_la_file,
                            dissolveby='LAD21NM',
                            search=local_auth))

    # Creating a Geo Dataframe of only stops in la
    la_stops_geo_df = (gs.find_points_in_poly
                            (geo_df=stops_geo_df,
                                polygon_obj=la_poly))

    # Make LA LSOA just containing local auth
    uk_la_file = uk_la_file[['LAD21NM', 'geometry']]

    # merge the two dataframes limiting to just the la
    #TODO: mismatch of 2020 LA's and 2021 LA's
    la_pop_df = whole_nation_pop_df.merge(uk_la_file,
                                            how='right',
                                            left_on='LAD20NM',
                                            right_on='LAD21NM',
                                            suffixes=('_pop', '_LSOA'))

    # subset by the local authority name needed
    la_pop_df=la_pop_df.loc[la_pop_df["LAD20NM"]==local_auth]                                        
  
    # rename the "All Ages" column to pop_count as it's the population count
    la_pop_df.rename(columns={"All Ages": "pop_count"}, inplace=True)

    # change pop_count to number (int)
    # bham_pop_df['pop_count'] = (pd.to_numeric
    #                             (bham_pop_df
    #                              .pop_count.str
    #                              .replace(",", "")))

    # Get a list of ages from config
    age_lst = config['age_lst']

    # Get a datframe limited to the data ages columns only
    age_df = dt.slice_age_df(la_pop_df, age_lst)

    # Create a list of tuples of the start and finish indexes for the age bins
    age_bins = dt.get_col_bins(age_lst)

    # get the ages in the age_df binned, and drop the original columns
    age_df = dt.bin_pop_ages(age_df, age_bins, age_lst)

    # Ridding the bham pop df of the same cols
    la_pop_df.drop(age_lst, axis=1, inplace=True)

    # merging summed+grouped ages back in
    la_pop_df = pd.merge(la_pop_df, age_df, left_index=True, right_index=True)
    # converting into GeoDataFrame
    la_pop_df = gpd.GeoDataFrame(la_pop_df)

    # create a buffer around the stops, in column "geometry" #forthedemo
    # the `buffer_points` function changes the df in situ
    la_stops_geo_df = gs.buffer_points(la_stops_geo_df)

    # renaming the column to geometry so the point in polygon func gets expected
    la_pop_df.rename(columns={"geometry_pop": "geometry"}, inplace=True)

    # import the disability data - this is the based on the 2011 census
    # TODO: use new csv_to_df func to make disability_df
    disability_df = pd.read_csv(os.path.join(CWD,
                                            "data",
                                            "nomis_QS303.csv"),
                                header=5)
    # drop the column "mnemonic" as it seems to be a duplicate of the OA code
    # also "All categories: Long-term health problem or disability" is not needed,
    # nor is "Day-to-day activities not limited"
    drop_lst = ["mnemonic",
                "All categories: Long-term health problem or disability",
                "Day-to-day activities not limited"]
    disability_df.drop(drop_lst, axis=1, inplace=True)
    # the col headers are database unfriendly. Defining their replacement names
    replacements = {"2011 output area": 'OA11CD',
                    "Day-to-day activities limited a lot": "disab_ltd_lot",
                    "Day-to-day activities limited a little": "disab_ltd_little"}
    # renaming the dodgy col names with their replacements
    disability_df.rename(columns=replacements, inplace=True)

    # Summing the two columns to get total disabled (which is what I thought
    #   "All categories:..." was!)
    disability_df["disb_total"] = (disability_df["disab_ltd_lot"]
                                + disability_df["disab_ltd_little"])

    # Importing the population data for each OA for 2011
    normal_pop_OA_2011_df = (pd.read_csv(
                            os.path.join
                            ("data","KS101EW-usual_resident_population.csv"),
                            header=6,
                            engine="python"))
    # Cutting out text at the end of the csv
    normal_pop_OA_2011_df = normal_pop_OA_2011_df.iloc[:-4]

    # Renaming columns for clarity and consistency before join
    normal_pop_OA_2011_df.rename(columns={'2011 output area':'OA11CD','2011':'population_2011'},inplace=True)

    # Casting population numbers in 2011 data as int
    normal_pop_OA_2011_df["population_2011"] = normal_pop_OA_2011_df["population_2011"].astype(int)

    # Joining the 2011 total population numbers on to disability df
    disability_df = pd.merge(disability_df, normal_pop_OA_2011_df, how='inner', left_on="OA11CD", right_on="OA11CD")

    # Ticket #97 - calculating the proportion of disabled people in each OA
    disability_df["proportion_disabled"] = (
                                            disability_df['disb_total'] 
                                            / 
                                            disability_df['population_2011']
                                            )

    # Slice disability df that only has the proportion disabled column and the OA11CD col
    disab_prop_df = disability_df[['OA11CD', 'proportion_disabled']]

    # Merge the proportion disability df into main the pop df with a left join
    la_pop_df = la_pop_df.merge(disab_prop_df, on='OA11CD', how="left")

    # Make the calculation of the number of people with disabilities in the year 
    # of the population estimates
    la_pop_df["number_disabled"] = (
                                    round
                                    (la_pop_df["pop_count"]
                                    *
                                    la_pop_df["proportion_disabled"])
                                    )
    la_pop_df["number_disabled"] = la_pop_df["number_disabled"].astype(int)

    # import the sex data
    # # TODO: use new csv_to_df func to make the sex_df
    # sex_df = pd.read_csv(os.path.join(CWD, "data", "nomis_QS104EW.csv"),
    #                      header=6,
    #                      usecols=["2011 output area",
    #                               "Males", "Females"])

    # sex_df = bham_pop_df['OA11CD', 'males_pop', 'fem_pop']


    # # # renaming the dodgy col names with their replacements
    replacements = {"males_pop": "male",
                    "fem_pop": "female"}
    la_pop_df.rename(columns=replacements, inplace=True)

    # # merge the sex data with the rest of the population data
    # bham_pop_df = bham_pop_df.merge(sex_df, on='OA11CD', how='left')

    # Make a polygon object from the geometry column of the stops df 
    # all_stops_poly = gs.poly_from_polys(birmingham_stops_geo_df)

    # # find all the pop centroids which are in the bham_stops_poly
    pop_in_poly_df = gs.find_points_in_poly(la_pop_df, la_stops_geo_df)
    # Dedupe the df because many OAs are appearing multiple times (i.e. they are served by multiple stops)
    pop_in_poly_df = pop_in_poly_df.drop_duplicates(subset="OA11CD")

    # Count the population served by public transport
    served = pop_in_poly_df.pop_count.sum()
    full_pop = la_pop_df.pop_count.sum()
    not_served = full_pop - served
    pct_not_served = "{:.2%}".format(not_served/full_pop)
    pct_served = "{:.2%}".format(served/full_pop)

    print(f"""The number of people who are served by public transport is {served}.\n
            The full population of {local_auth} is calculated as {full_pop}
            While the number of people who are not served is {not_served}""")

    la_results_df=pd.DataFrame({"LA":[local_auth],
                            "pop":[full_pop],
                            "pop_served":[served],
                            "pop_not_served":[not_served],
                            "pct_served":[pct_served],
                            "pct_not_served":[pct_not_served]})

    # Output this iteration's df to the dict
    total_df_dict[local_auth]=la_results_df


    # # Disaggregations!
    pd.set_option("precision", 1)

    # Calculating those served and not served by age
    age_bins_ = ['0-4', '5-9', '10-14', '15-19', '20-24',
                '25-29', '30-34', '35-39', '40-44', '45-49', '50-54',
                '55-59', '60-64', '65-69', '70-74', '75-79',
                '80-84', '85-89', '90+']

    age_servd_df = dt.served_proportions_disagg(pop_df=la_pop_df,
                                                pop_in_poly_df=pop_in_poly_df,
                                                cols_lst=age_bins_)
    # Create an LA column 
    age_servd_df["LA"]=local_auth

    print("\n\n==========Age Disaggregation===========\n\n")

    # Output this iteration's age df to the dict
    age_df_dict[local_auth]=age_servd_df

    # print(age_servd_df)

    # # Calculating those served and not served by sex
    sex_cols = ['male', 'female']

    sex_servd_df = dt.served_proportions_disagg(pop_df=la_pop_df,
                                                pop_in_poly_df=pop_in_poly_df,
                                                cols_lst=sex_cols)

    # Create an LA column 
    sex_servd_df["LA"]=local_auth

    print("\n\n==========Sex Disaggregation===========\n\n")

    # print(sex_servd_df)

    # Output this iteration's age df to the dict
    sex_df_dict[local_auth]=sex_servd_df

    # Calculating those served and not served by disability
    disab_cols = ["number_disabled"]

    disab_servd_df = dt.served_proportions_disagg(pop_df=la_pop_df,
                                                pop_in_poly_df=pop_in_poly_df,
                                                cols_lst=disab_cols)

    # Create an LA column 
    disab_servd_df["LA"]=local_auth

    print("\n\n==========Disability Disaggregation===========\n\n")

    # Output this iteration's age df to the dict
    disab_df_dict[local_auth]=disab_servd_df

    print(disab_servd_df)

    # Calculating those served and not served by urban/rural
    urb_col = ["urb_rur_class"]

    # Filtering by urban and rural to make 2 dfs
    urb_df = la_pop_df[la_pop_df.urb_rur_class == "urban"]
    rur_df = la_pop_df[la_pop_df.urb_rur_class == "rural"]

    # Because these dfs a filtered to fewer rows, the pop_in_poly_df must be
    # filtered in the same way
    urb_pop_in_poly_df = (urb_df.merge(pop_in_poly_df,
                        on="OA11CD", how="left")
                        .loc[:, ['OA11CD', 'pop_count_y']])
    urb_pop_in_poly_df.rename(columns={'pop_count_y': 'pop_count'}, inplace=True)
    rur_pop_in_poly_df = (rur_df.merge(pop_in_poly_df,
                        on="OA11CD", how="left")
                        .loc[:, ['OA11CD', 'pop_count_y']])
    rur_pop_in_poly_df.rename(columns={'pop_count_y': 'pop_count'}, inplace=True)

    urb_servd_df = dt.served_proportions_disagg(pop_df=urb_df,
                                                pop_in_poly_df=urb_pop_in_poly_df,
                                                cols_lst=['pop_count'])

    rur_servd_df = dt.served_proportions_disagg(pop_df=rur_df,
                                                pop_in_poly_df=rur_pop_in_poly_df,
                                                cols_lst=['pop_count'])


    print("\n\n==========Urban/Rural Disaggregation===========\n\n")

    print("Urban")
    print(urb_servd_df)
    print("Rural")
    print(rur_servd_df)

        # Create an LA column 
    urb_servd_df["LA"]=local_auth
    rur_servd_df["LA"]=local_auth

    print("\n\n==========Disability Disaggregation===========\n\n")

    # Output this iteration's urb and rur df to the dict
    urb_df_dict[local_auth]=urb_servd_df
    rur_df_dict[local_auth]=rur_servd_df

all_la = pd.concat(total_df_dict.values())
sex_all_la = pd.concat(sex_df_dict.values())
urb_all_la = pd.concat(urb_df_dict.values())
rur_all_la = pd.concat(rur_df_dict.values())
disab_all_la = pd.concat(disab_df_dict.values())
age_all_la = pd.concat(age_df_dict.values())
print(disab_all_la)
print(age_all_la)
print(rur_all_la)
print(urb_all_la)

all_la=all_la.reset_index()

output_tabs={}

output_tabs["local_auth"] = gpt.GPTable(
                                table=all_la,
                                title="local_auth",
                                scope=None,
                                units=None,
                                source="Office for National Statistics"
                                )

gpt.write_workbook(filename="SDG.xlsx",
                    sheets=output_tabs,
                    auto_width=True)