# core
import os 
import random
import time 

# third party
import yaml
import pandas as pd
import geopandas as gpd

# our modules 
import geospatial_mods as gs
import data_ingest as di
import data_transform as dt
import data_output as do

# timings
start = time.time()
# get current working directory
CWD = os.getcwd()
# TODO: find out best practice on CWD

# Load config
with open(os.path.join(CWD, "config.yaml")) as yamlfile:
    config = yaml.load(yamlfile, Loader=yaml.FullLoader)
    module = os.path.basename(__file__)
    print(f"Config loaded in {module}")

# Constants
DEFAULT_CRS = config["DEFAULT_CRS"]
DATA_DIR = config["DATA_DIR"]

pop_year = "2011"
boundary_year = "2021"


# Get the pandas dataframe for the stops data
stops_df = di.get_stops_file(url=config["NAPTAN_API"],
                             dir=os.path.join(os.getcwd(),
                                              "data",
                                              "stops"))
# filter out on inactive stops
filtered_stops = dt.filter_stops(stops_df=stops_df)

# coverts from pandas df to geo df
stops_geo_df = (di.geo_df_from_pd_df(pd_df=filtered_stops,
                                     geom_x='Easting',
                                     geom_y='Northing',
                                     crs=DEFAULT_CRS))

# adds in high/low capacity column
stops_geo_df = dt.add_stop_capacity_type(stops_df=stops_geo_df)

# get usual population for scotland
usual_pop_path = os.path.join(CWD, "data", "KS101SC.csv")
sc_usual_pop = di.read_usual_pop_scotland(usual_pop_path)

# getting path for .shp file for LA's
uk_la_path = di.get_shp_abs_path(dir=os.path.join(os.getcwd(),
                                                   "data",
                                                   "LA_shp",
                                                   boundary_year))

# getting the coordinates for all LA's
uk_la_file = di.geo_df_from_geospatialfile(path_to_file=uk_la_path)
sc_la_file = uk_la_file[uk_la_file["LAD21CD"].str[0].isin(['S'])]

# Get population weighted centroids into a dataframe
sc_pop_wtd_centr_df = (di.geo_df_from_geospatialfile
                       (os.path.join
                        (DATA_DIR,
                         'pop_weighted_centroids',
                         "scotland",
                         pop_year,
                         "OutputArea2011_PWC.shp")))

# get weighted centroids and merge with population
pwc_with_pop = pd.merge(left=sc_usual_pop,
                             right=sc_pop_wtd_centr_df,
                             left_on=sc_usual_pop.index,
                             right_on="code",
                             how="left")

# OA to LA lookup
oa_to_la_lookup_path = os.path.join(CWD, "data","oa_la_mapping",
                                    "scotland", boundary_year,
                                    "PCD_OA_LSOA_MSOA_LAD_NOV21_UK_LU.csv")

# reads in the OA to LA lookupfile 
oa_to_la = pd.read_csv(oa_to_la_lookup_path, encoding="ISO-8859-1",
                        usecols=["oa11cd","ladnm"])

# dedeup OA to LA as original data includes postcodes etc..
oa_to_la_deduped = oa_to_la.drop_duplicates(subset="oa11cd")
                     
# merges the pwc with it's corresponding LA
pwc_with_pop_with_la = pd.merge(left=pwc_with_pop,
                                right=oa_to_la_deduped,
                                left_on="code",
                                right_on="oa11cd",
                                how="left")

# Unique list of LA's to iterate through
list_local_auth = sc_la_file["LAD21NM"].unique()
random_la = random.choice(list_local_auth)
sc_auth = [random_la]

# define output dicts to capture dfs
total_df_dict = {}
sex_df_dict = {}
disab_df_dict = {}

for local_auth in sc_auth:
    print(f"Processing: {local_auth}")
    
    # Get a polygon of la based on the Location Code
    la_poly = (gs.get_polygons_of_loccode(
        geo_df=sc_la_file,
        dissolveby="LAD21NM",
        search=local_auth))

    # Creating a Geo Dataframe of only stops in la
    la_stops_geo_df = (gs.find_points_in_poly
                       (geo_df=stops_geo_df,
                        polygon_obj=la_poly))
    
    # buffer around the stops
    la_stops_geo_df = gs.buffer_points(la_stops_geo_df)

    # filter only by current la 
    only_la_pwc_with_pop = gpd.GeoDataFrame(pwc_with_pop_with_la[pwc_with_pop_with_la["ladnm"]==local_auth])
    

    # find all the pop centroids which are in the la_stops_geo_df
    pop_in_poly_df = gs.find_points_in_poly(only_la_pwc_with_pop, la_stops_geo_df)
    
    # Deduplicate the df as OA appear multiple times 
    pop_in_poly_df = pop_in_poly_df.drop_duplicates(subset="oa11cd")

    # all the figures we need
    served = pop_in_poly_df["All people"].astype(int).sum()
    full_pop = only_la_pwc_with_pop["All people"].astype(int).sum()
    not_served = full_pop - served
    pct_not_served = "{:.2f}".format(not_served/full_pop*100)
    pct_served = "{:.2f}".format(served/full_pop*100)

    print(f"""The number of people who are served by public transport is {served}.\n
            The full population of {local_auth} is calculated as {full_pop}
            While the number of people who are not served is {not_served}""")

    # putting results into dataframe
    la_results_df = pd.DataFrame({"All_pop":[full_pop],
                                  "Served":[served],
                                  "Unserved":[not_served],
                                  "Percentage served":[pct_served],
                                  "Percentage unserved":[pct_not_served]})
    
      
    # Re-orienting the df to what's accepted by the reshaper and renaming col
    la_results_df = la_results_df.T.rename(columns={0:"Total"})

    # Feeding the la_results_df to the reshaper
    la_results_df_out = do.reshape_for_output(la_results_df,
                                              id_col="Total",
                                              local_auth=local_auth)

    # Finally for the local authority totals the id_col can be dropped
    # That's because the disaggregations each have their own column, 
    # but "Total" is not a disaggregation so doesn't have a column.
    # It will simply show up as blanks (i.e. Total) in all disagg columns
    la_results_df_out.drop("Total", axis=1, inplace=True)
    
    
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
    sex_df_dict[local_auth]=sex_servd_df_out

    # Output this iteration's df to the dict
    total_df_dict[local_auth] = la_results_df_out
    
    ## Disability disaggregation
    
    disability_df = pd.read_csv(os.path.join(CWD,
                                             "data",
                                             "QS303_scotland.csv"))
    # drop the column "geography code" as it seems to be a duplicate of "geography" 
    # also "All categories: Long-term health problem or disability" is not needed,
    # nor is "year" as we know estimates are for 2011.
    drop_lst = ["date", "geography code",
                "Disability: All categories: Long-term health problem or disability; measures: Value"]
    disability_df.drop(drop_lst, axis=1, inplace=True)
    # the col headers are database unfriendly. Defining their replacement names
    replacements = {"geography": 'oa11cd',
                    "Disability: Day-to-day activities limited a lot; measures: Value": "disab_ltd_lot",
                    "Disability: Day-to-day activities limited a little; measures: Value": "disab_ltd_little",
                    'Disability: Day-to-day activities not limited; measures: Value': "disab_not_ltd"}
    # renaming the dodgy col names with their replacements
    disability_df.rename(columns=replacements, inplace=True)
    
    # Getting the disab total
    disability_df["disb_total"] = (disability_df["disab_ltd_lot"]
                                   + disability_df["disab_ltd_little"])

    # Calcualting the total "non-disabled"
    la_pop_only = only_la_pwc_with_pop[['oa11cd','All people']]
    disability_df = la_pop_only.merge(disability_df, on="oa11cd")
    # Putting the result back into the disability df
    disability_df["non-disabled"] = disability_df["All people"] - disability_df['disb_total']

    # Calculating the proportion of disabled people in each OA
    disability_df["proportion_disabled"] = (
        disability_df['disb_total']
        /
        disability_df['All people']
    )
    
    # Calculating the proportion of non-disabled people in each OA
    disability_df["proportion_non-disabled"] = (
        disability_df['non-disabled']
        /
        disability_df['All people']
    )
    
    # Slice disability df that only has the proportion disabled column and the OA11CD col
    disab_prop_df = disability_df[['oa11cd', 'proportion_disabled', 'proportion_non-disabled']]

    # Merge the proportion disability df into main the pop df with a left join
    only_la_pwc_with_pop = only_la_pwc_with_pop.merge(disab_prop_df, on='oa11cd', how="left")

    # Make the calculation of the number of people with disabilities in the year
    # of the population estimates
    only_la_pwc_with_pop["number_disabled"] = (
        round
        (only_la_pwc_with_pop["All people"]
         *
         only_la_pwc_with_pop["proportion_disabled"])
    )
    only_la_pwc_with_pop["number_disabled"] = only_la_pwc_with_pop["number_disabled"].astype(int)

    # Make the calculation of the number of non-disabled people in the year
    # of the population estimates
    only_la_pwc_with_pop["number_non-disabled"] = (
        round
        (only_la_pwc_with_pop["All people"]
         *
         only_la_pwc_with_pop["proportion_non-disabled"])
    )
    only_la_pwc_with_pop["number_non-disabled"] = only_la_pwc_with_pop["number_non-disabled"].astype(int)
    
# every single LA
all_la = pd.concat(total_df_dict.values())
sex_all_la = pd.concat(sex_df_dict.values())


# Stacking the dataframes
all_results_dfs = [all_la, sex_all_la]
final_result = pd.concat(all_results_dfs)
final_result["Year"] = pop_year

# Outputting to CSV

final_result.to_csv("Scotland_results.csv", index=False)

end = time.time()

print(f"This took {(end-start)/60} minutes to run")




