import runpy
import logging
import sys
import os

from data_ingest import persistent_exists, GCPBucket

# Add the parent directory of 'src' to the Python import path
sys.path.append(os.path.dirname(os.path.realpath(__file__)))


run_timetables = True
pre_processing = True
countries = ['eng_wales', 'scotland', 'northern_ireland']

# Set up logging 
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

make_new_highly_serviced_stops = False

bucket = GCPBucket()

if run_timetables:
    # check if highly serviced stops feather exists
    hss_bus_path = "data/england_bus_timetable/bus_highly_serviced_stops.feather"
    hss_train_path = "data/england_train_timetable/train_highly_serviced_stops.feather"
    
    if not persistent_exists(hss_bus_path):
        logger.info('Running bus timetable pipeline')
        
        if make_new_highly_serviced_stops:
            logger.info('Making new highly serviced stops')
            runpy.run_module('time_table.SDG_bus_timetable', run_name='__main__')
        else:
            # Download the highly serviced stops from google storage
            logger.info('Downloading highly serviced stops')
            bucket.download_file(hss_bus_path, hss_bus_path)      
    else:
        logger.info('Bus highly serviced stops already exists')
    
    if not persistent_exists(hss_train_path):
        logger.info('Running train timetable pipeline')
        if make_new_highly_serviced_stops:
            logger.info('Making new highly serviced train stops')
            runpy.run_module('time_table.SDG_train_timetable', run_name='__main__')
        else:
            # Download the highly serviced stops from google storage
            logger.info('Downloading highly serviced stops')
            bucket.download_file(hss_train_path, hss_train_path)
    else:
        logger.info('Train highly serviced stops already exists')

# Run pre-processing pipeline      
if pre_processing:
    logger.info('Running pre-processing pipeline')
    runpy.run_module('pre_processing.eng_wales_pre_process', run_name='__main__')  

logger.info("Running country specific pipelines")
for country in countries:
    logger.info(f'Running pipeline for {country}')
    runpy.run_module(f'SDG_{country}', run_name='__main__')

logger.info("Pipeline complete")