# Calculation of Statistics for SDG 11.2.1: The UK Method Compared to the EU Method


## Differences and similarities between our method and that of the EU

The following is a comparison of the method employed to calculate public transport accessibility by a team researching for the EU Commission and a UK team, working on behalf of the Sustainable Development Goals team in the Office for National Statistics (ONS). The two teams will be referred to as the “ONS team” and the “EU team” going forward. 


## Technology used


### Open source development 

The SDG team at ONS signed up to the Inclusive Data Charter[^1]. In line with its principles, those of the SDGs and [OSAT](https://link.springer.com/article/10.1007/s10668-012-9337-9) we have worked as transparently as possible, making all code open source and conversations around method and development decisions published on our Github repository, and used free and open source tools and technology throughout the development process. The EU team have made their code available (see annexes [here](https://ec.europa.eu/regional_policy/en/information/publications/working-papers/2020/low-carbon-urban-accessibility)) as well as a [write up](https://ec.europa.eu/regional_policy/sources/docgener/work/012020_low_carbon_urban.pdf) about their chosen method and results, though an open development process was not found, though it may exist. 


### Software

The EU team and the ONS 11.2.1 project team have approached the challenges of calculating transport accessibility similarly, both in the methodology and the technology involved. Both teams have made use of Python as a means to ingest and process data. However the main processing of the data is done using different technologies, namely that the EU team has used the ArcPy library to interact with ArcGIS, a program which is not open source or free. The ONS team, by comparison, carries out the data processing in [Pandas](https://pandas.pydata.org) and the Geospatial analysis in [GeoPandas](https://geopandas.org/en/stable/), all of which are open source and free. 


### Computation method

The ONS computation calculates the service accessibility (see [definitions](https://onsdigital.github.io/SDG_11.2.1/html/write_up.html#definitions)) for the whole country - except that currently the script cycles through local authorities and aggregates the data into a single table later. This method of calculating areas and combining up to the whole nation later will possibly be removed when the ONS team optimise the script. 

The ONS pipeline calculates the service accessibility for the whole country by cycling through local authorities and aggregating at the end of the process (for version 1.0 of our pipeline). Version 1.0 will only use data for 2011 and 1.1 will make the calculation over multiple years. 

The type of computation may be changed in future iterations of the project to a more vectorised computation (probably in version 1.3) - if [this feature ](https://github.com/ONSdigital/SDG_11.2.1/issues/159)is implemented. The EU team calculates the transport service accessibility for each “area of interest” (e.g. city, region,urban centre, etc.) focusing on the urban area (in line with 11.2.1 methodology) but the ONS team has calculated for rural areas too - output data that is then compared with urban areas to get an idea of comparative transport access. 


## The methodology for the calculation used

The method used for the calculation is also very similar, but there are some important differences. 


### Transport access points

The data on the location of the transport nodes (stops and stations) is required for this calculation. The ONS team sourced the data from the [National Public Transport Access Nodes (NaPTAN)](https://beta-naptan.dft.gov.uk/) data set. It is not clear where the EU team sourced their data from. 

The EU team cluster stops together if they are within 50 metres of each other. The cluster is then represented by a single point, mid distance between the stops that have been clustered. On the other hand, the ONS team treats every stop in the country as individual stops, each of which is treated as an individual point. To establish the effect on the results this would have, our team would have to update our code to cluster stops in the same way, [a step the ONS team will consider](https://github.com/ONSdigital/SDG_11.2.1/issues/217) for future iterations of the project. 


### Public transport service areas

When it comes to calculating the service area, both teams use the same distance as a buffer. The difference is how the distance from the central node (station/stop/cluster) is calculated. In the EU team’s method, a service area (catchment areas) of 500 metres or 1000m (according to capacity) is created. This is made using a pedestrian path and road network. The ONS team use a simple Euclidean buffer to create a circle (techinically a  polygon) around each stop. The polygons are then combined to create the entire service area. Our team has considered the network/path method, and it has not been ruled out, however the ONS team note that computationally this will be more intensive and accurate results rely on a very accurate pedestrian path network. If the network is incomplete, or maps paths on to pedestrian unfriendly roads (such as fast roads with no pavements) the results will be unreliable.

Examining the EU method, by using the road network, based on the shape_length parameter (which is probably the path line) and the walking speed, an isochrone of the service area is created. This is almost certainly how the above-mentioned service area polygons are created. The current ONS method makes no use of isochrones or irregular polygons in the calculation of service areas. 


### Calculation of served vs. unserved population

Seemingly the method of counting the population inside and outside of the service areas is the same on both teams, though the method of establishing the population in each area is different. 

Both the ONS team and the EU team count the population inside and outside of the service areas. The ONS team use sum method in Pandas dataframe, after running a points in polygons query (a kind of spatial join) to only include population centres that are within the population centre. 


### Differentiating high and low capacity stops

The ONS team have differentiated high and low capacity stops using the StopType field in the NaPTAN dataset, the feasibility and methodology of which was discussed [here](https://github.com/ONSdigital/SDG_11.2.1/issues/177) and [here](https://github.com/ONSdigital/SDG_11.2.1/issues/207); and the implementation of these features are discussed [here](https://github.com/ONSdigital/SDG_11.2.1/issues/196) and [here](https://github.com/ONSdigital/SDG_11.2.1/issues/176).

The EU team imports two datasets of high and low capacity stops. However it is unclear how those datasets are generated. 

The ONS team group underground service (in London) at the same level as Trams (e.g in Manchester), however this may change based on further discussion before the release of version 1.0.  


## Similarities and differences in our data


### Spatial granularity of data

The ONS team have used population data at [output area](https://www.ons.gov.uk/methodology/geography/ukgeographies/censusgeography#output-area-oa) level, which is the most granular level available. The Government Statistical Service states that the 2011 Census, England was divided into 171,372 Output Areas (OAs) which on average have a resident population of 309 people. 

According to their publication the EU team population estimate figures at the building-block level and combine that data at the best available spatial resolution with data on land cover, land use, and data on the location, function and height of buildings to obtain estimates of a useful quality.


### Urban and rural definitions

The ONS definition of "Urban" and "Rural" is defined by their population density in each output area. According to Government Statistical Service, **urban** areas are the connected built up areas identified by Ordnance Survey mapping that have resident populations \
 above 10,000 people (2011 Census). On the other hand **rural** areas are those areas that are not urban, i.e. consisting of settlements below 10,000 people or are open countryside.

The EU team have used the EU definition of urban, which states that urban centres have a population density of more than 1 500 inhabitants/km².” (Poelman et al., pg. 30). Additionally they use the [EU-OECD “Functional Urban Area” definition](https://www.oecd.org/cfe/regionaldevelopment/functional-urban-areas.htm) for urban centres, for which is the conglomerate they calculate.


## Disaggregations

Age and sex

The ONS team disaggregates on age and sex. Having used the UK census population data which is detailed all ages and sex of population at output level up to 90+. The population data was then “binned” into 5 year groups based on a discussion with the SDG data team **[noted here](https://github.com/ONSdigital/SDG_11.2.1/issues/7#issuecomment-734258025)** (Nov, 2020). The binning can be changed as it is defined in the config file; though this will be improved and fully tested in a [future iteration](https://github.com/ONSdigital/SDG_11.2.1/issues/216). 

The EU team intends to use the European population grid for 2021 for disaggregated breakdowns, but in the [most recent publication](https://ec.europa.eu/regional_policy/sources/docgener/work/012020_low_carbon_urban.pdf), they did not carry out disaggregated analysis by age, nor sex. 

Disability status

NOMIS is a research database and analytical tool that provides data on labour markets, demographics, and the economy. NOMIS stands for "New Opportunities for Migrants' Integration and Success" and is funded by the Economic and Social Research Council (ESRC) in the United Kingdom. Via [NOMIS](https://www.nomisweb.co.uk/), data on disability status output from the UK census data is available for the year of the census. For years later than the census, the population estimate for each area was multiplied by the proportion of each disability category, calculated from the census year (see the [methodology writeup on disability](https://onsdigital.github.io/SDG_11.2.1/html/write_up.html#disability-status)). 

Again, the EU team did not carry out disaggregated analysis on disability across the EU due to a lack of data.


<!-- Footnotes themselves at the bottom. -->
## Notes

[^1]: [Inclusive data charter action plan for the global Sustainable Development Goals](https://www.ons.gov.uk/economy/environmentalaccounts/methodologies/inclusivedatacharteractionplanfortheglobalsustainabledevelopmentgoals)
