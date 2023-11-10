# Timetable-based filtering of transport nodes

The frequency of service at transport nodes, such as stops and stations, plays a critical role in promoting sustainable and efficient transportation systems. By filtering these nodes based on their timetable frequency, we can identify areas with infrequent service, where accessibility and reliability may be compromised. Our method filters out any nodes with a frequency of less than 1 service per hour, between the hours of 6am to 8pm on weekdays. This is equivalent to a service every 60 minutes, which is a common threshold for low-frequency service in many cities around the world.

We aim to produce accurate statistics which contribute to the achievement of SDG 11.2.1 by creating a sustainable and inclusive transport system that provides frequent and reliable services to all individuals, thereby improving accessibility and promoting sustainable urban and rural mobility.

## Parsing Train Timetable Data

Train timetable data plays a crucial role in the filtering methodology for transport nodes based on timetable frequency. In our project, we utilize train timetable data obtained from the Association of Train Operating Companies (ATOC). This data is provided in the CIF (Common Interface Format) format and is updated on a weekly basis, reflecting the most recent train schedules. To facilitate data analysis and processing, a data dictionary containing file and field descriptions is made available by ATOC which the code in our parser was based on.

To access the train timetable data, users are required to download the latest CIF files from ATOC. However, due to the fact that file names are changing with each update, it is not feasible to automatically retrieve the data each time the pipeline is run. To address this, our project incorporates the SDG_train_timetable module, which pre-processes the user-downloaded CIF files and saves the output in a format that is ready to be utilised within the main data pipeline.

The train timetable data that is released by ATOC has multiple datasets, but for the purpose of our analysis we are only interested in two specific files: the .msn (Master Station Names) and .mca files. These files provide essential information, such as station names and unique identifiers, which we later use in the filtering process and to locate the stations accurately. The location data within the CIF files is represented in the form of grid references, without the corresponding 100km grid square information which makes it impossibly to plot the station locations accurately. To address this issue, we ultimately used the TIPLOC codes, which meant that we could join our train data back onto Naptan data - see the section "Locating Stations with Tiploc Codes" for more details.

The two files (.msn and .mca) have specific structures that contain valuable information for our analysis. To process the data more efficiently, we parsed the data from each file into their respective dataframes.

Within the .msn file, we focused on extracting the departure times (scheduled) for each stop. If the departure time was empty, we disregarded any attempts to extract public or scheduled pass times, as they were deemed irrelevant for our filtering methodology. Additionally, any entries marked as BSD (Backward Set Data) were ignored, as these entries indicate that the data has been deleted or replaced.

In the case of time values with an 'H' appended were rounded up or down half a minute.

To streamline the analysis, any records with empty times were removed from the dataset. This ensured that only relevant and complete records were included in subsequent filtering and analysis processes. Furthermore, we filtered the records to include only activities with the code 'T' (i.e., timing), as other activities were considered extraneous to the timetable frequency analysis.

By organizing and filtering the data based on these criteria, we were able to create a refined dataset of rail stations based on the frequency of the service at each station which aligns with SDG 11.2.1 methodology.

## Locating Stations with Tiploc Codes

During our data analysis, a mismatch was discovered between the number of stations listed in the external location data file and the number of station codes (CRS codes) obtained from the National Rail database. This mismatch was due to the fact that the external location data file contained stations that were no longer in operation. 

Our method used TIPLOC codes to locate stations geographically; Tiploc means “timing point locations” and are a unique identifier for train stations (and other points). We used them because we found that they were embedded in Atoccode, which was in the Naptan data that we used for all stop and station locations. By extracting [4:] in the Atcocode column, you get the Tiploc code - which was useful for joining back onto train data, so we got (lat, lon) locations. 


# Parsing the Bus Timetable Data

# Bus Timetable Data

To complement the train timetable data, we also incorporate bus timetable data into our filtering methodology. The bus timetable data is sourced from the Bus Open Data Service managed by the Department for Transport (DFT). This service provides access to up-to-date information on bus schedules across the country. The dataset is updated daily at approximately 06:00 GMT, and we specifically download the GTFS (General Transit Feed Specification) schedule dataset, which is provided in the form of zipped text files.

It is important to note that the process of downloading the bus timetable data is separate from the main pipeline of our methodology. This means that it is a one-time task and not automatically performed as part of the regular data processing. To download new data, the user needs to:

a) update the filename in the configuration file
b) set the download flag to true. 

With these changes if the current data is more than seven days old, the latest dataset is automatically downloaded to ensure that the analysis incorporates the most recent bus schedule information.

For comprehensive documentation of the available files and field names, users can refer to the data dictionaries provided by the Bus Open Data Service. These resources offer detailed insights into the structure and meanings of the various files within the GTFS schedule dataset.

The bus timetable data consists of multiple files, each serving a specific purpose within the dataset. An overview of the available files is as follows:

1. `Agency.txt`: This file provides information about the transit agency, including its name, URL, and contact details.

2. `Stops.txt`: Contains a list of all the bus stops, along with their unique identifiers, names, and geographical coordinates.

3. `Routes.txt`: Describes the routes taken by buses, including their unique identifiers, names, and associated agencies.

4. `Trips.txt`: Provides details about individual trips made by buses, including their unique identifiers, routes, and associated service IDs.

5. `Stop Times.txt`: Contains the timetable information for each stop, including the arrival and departure times for buses.

6. `Calendar.txt`: Describes the service availability for each day of the week, specifying the start and end dates for each service.

7. `Calendar Dates.txt`: Provides additional service availability information, including exceptions or changes to the regular schedule.

8. `Fare Attributes.txt`: Contains fare-related information, such as fare identifiers and prices.

9. `Fare Rules.txt`: Specifies the fare rules and their applicability to different routes or trips.

# Bus Timetable Data

To complement the train timetable data, we also incorporate bus timetable data into our filtering methodology. The bus timetable data is sourced from the Bus Open Data Service managed by the Department for Transport (DFT). This service provides access to up-to-date information on bus schedules across the country. The dataset is updated daily at approximately 06:00 GMT, and we specifically download the GTFS (General Transit Feed Specification) schedule dataset, which is provided in the form of zipped text files.

It is important to note that the process of downloading the bus timetable data is separate from the main pipeline of our methodology. This means that it is a one-time task and not automatically performed as part of the regular data processing. To download new data, the user needs to update the filename in the configuration file and set the download flag to true. Additionally, if the current data is more than seven days old, the latest dataset is automatically downloaded to ensure that the analysis incorporates the most recent bus schedule information.

For comprehensive documentation of the available files and field names, users can refer to the data dictionaries provided by the Bus Open Data Service. These resources offer detailed insights into the structure and meanings of the various files within the GTFS schedule dataset.

The bus timetable data consists of multiple files, each serving a specific purpose within the dataset. An overview of the available files is as follows:

1. `Agency.txt`: This file provides information about the transit agency, including its name, URL, and contact details.

2. `Stops.txt`: Contains a list of all the bus stops, along with their unique identifiers, names, and geographical coordinates.

3. `Routes.txt`: Describes the routes taken by buses, including their unique identifiers, names, and associated agencies.

4. `Trips.txt`: Provides details about individual trips made by buses, including their unique identifiers, routes, and associated service IDs.

5. `Stop Times.txt`: Contains the timetable information for each stop, including the arrival and departure times for buses.

6. `Calendar.txt`: Describes the service availability for each day of the week, specifying the start and end dates for each service.

7. `Calendar Dates.txt`: Provides additional service availability information, including exceptions or changes to the regular schedule.

8. `Fare Attributes.txt`: Contains fare-related information, such as fare identifiers and prices.

9. `Fare Rules.txt`: Specifies the fare rules and their applicability to different routes or trips.

Data dictionaries of the available files and field names can be found [here](https://gtfs.org/schedule/reference/).


Our analysis and filtering methodology, we focus specifically on three key datasets: calendar.txt, stop_times.txt, and trips.txt. These datasets contain all the necessary information we require to analyze timetable frequency and make informed decisions.

The stop_times.txt dataset is crucial for our analysis as it contains the specific timetable information for each bus stop. It includes details such as the arrival and departure times of buses at each stop. This dataset enables us to extract the timetable frequency and assess the regularity of bus services at each stop.

The trips.txt dataset provides essential information about individual bus trips, including the unique identifiers of the trips, associated routes, and service IDs. This dataset allows us to link the timetable information from stop_times.txt with the corresponding trips and routes. By connecting these datasets, we can accurately map the bus trips to their respective schedules and understand the relationship between stops, trips, and routes.

To extract highly serviced stops (a bus stop that has 1 or more buses use it between 06:00 and 22:00 during the week) we do the following:

The three datasets, calendar.txt, stop_times.txt, and trips.txt, are processed and organized into separate dataframes to facilitate further analysis in our methodology. One of the initial filtering steps involves selecting departure times within specific hours that we have defined as indicative of highly serviced periods (as specified in the configuration file). This filtering process helps remove any spurious data, such as times recorded after 24:00.

To enhance data consistency and compatibility, the departure times are converted into dates. This conversion allows for easier comparison and manipulation of the data based on specific dates rather than individual time points.

The next step involves joining the stop_times, trips, and calendar datasets together, leveraging their interconnectedness. Unnecessary columns that do not contribute to our analysis are dropped from the combined dataset, ensuring a more streamlined and focused dataset for subsequent processing.

To analyse the bus services on a specific day, we apply a filtering mechanism based on a designated day of the week. By default, this filtering is set to Wednesday but can be adjusted by the user according to their requirements. Additionally, a function is implemented to filter services based on a specific date, providing flexibility for future use cases. This function takes as input a desired day of the week and identifies the corresponding date within the middle week between the earliest start date and the latest end date.

For instance, suppose the earliest start date is January 1, 2022, the latest end date is January 31, 2022, and the desired day of the week is Wednesday. In that case, the function would select Wednesday, January 12th as the representative date for the specified day of the week.