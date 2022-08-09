# core imports
import os

# third party import 
import requests
import yaml

# module imports
import data_ingest as di

# get current working directory
CWD = os.getcwd()

# Load config
with open(os.path.join(CWD, "config.yaml")) as yamlfile:
    config = yaml.load(yamlfile, Loader=yaml.FullLoader)
    module = os.path.basename(__file__)
    print(f"Config loaded in {module}")

# gets the northern ireland bus stops data from the api
ni_bus_stop_url = config["NI_stops_data"]
output_ni_csv = os.path.join(CWD,"data","Stops","NI","bus_stops_ni.csv")
di.get_ni_bus_stops_from_api(url=ni_bus_stop_url,
                                           output_path=output_ni_csv)

# reads in the NI bus stop data as geo df
ni_bus_stops = di.read_ni_bus_stops(output_ni_csv)
ni_bus_stops.head()
