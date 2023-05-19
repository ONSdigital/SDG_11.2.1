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
