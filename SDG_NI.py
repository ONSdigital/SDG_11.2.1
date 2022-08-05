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

# gets the northern ireland data from the api
ni_bus_stop_url = config["NI_stops_data"]
outut_ni_csv = os.path.join(CWD,"data","Stops","NI","Stops_NI.csv")
ni_bus_stop = di.get_ni_bus_stops_from_api(url=ni_bus_stop_url,
                                           output_path=outut_ni_csv)

