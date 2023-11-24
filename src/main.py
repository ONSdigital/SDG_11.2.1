import runpy
import logging


run_timetables = True
pre_processing = True
countries = ['eng_wales', 'scotland', 'northern_ireland']

# Set up logging 
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


if run_timetables:
    logger.info('Running bus timetable pipeline')
    runpy.run_module('SDG_bus_timetable', run_name='__main__')
    
    logger.info('Running rail timetable pipeline')
    runpy.run_module('SDG_rail_timetable', run_name='__main__')

if pre_processing:
    logger.info('Running pre-processing pipeline')
    runpy.run_module('eng_wales_pre_process', run_name='__main__')  

logger.info("Running country specific pipelines")
for country in countries:
    logger.info(f'Running pipeline for {country}')
    runpy.run_module(f'SDG_{country}', run_name='__main__')

logger.info("Pipeline complete")