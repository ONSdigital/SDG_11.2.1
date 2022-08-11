**Data Quality**

**NAPTAN**

The NaPTAN data is open source and available for anyone under a Government License.

In our methodology we need to distinguish between high and low capacity transport. There is no specific marker for this in the dataset. Therefore we use the stop type variable to determine whether a stop is high or low capacity transport. Trams and Metro have the same stop_type. According to the UN definition, Metros are high capacity and Trams are low capacity. However we are unable to distinguish between the two in our data. Here is the [issue](https://github.com/ONSdigital/SDG_11.2.1/issues/207) in more detail.

The data is submitted on an adhoc basis by local authorities. Details of their last submission can be seen [here](https://naptan.app.dft.gov.uk/Reports/frmStopsSummaryReport). As this is done on an adhoc basis there needs to be an element of downloading and timestamping data to ensure analysis is reproducible. Furthermore, the API which we get the data from is updated on a daily basis and it is difficult to tell whether the data has changed from the last upload.

There is a column called status which determines whether a stop is inactive or active. We use this variable to ensure all stops used in our calculation are active. However, there are some other values such as pending and blank which are ambiguous. A conversation with the department of transport documented [here](https://github.com/ONSdigital/SDG_11.2.1/issues/178) clarified how to deal with each value.

Even though it is the local authorities responsibility to submit to NaPTAN. It is unclear how they submit this information. Do they liaise with the bus companies to get this information and how timely is each submission?

An important issue is that it is not clear whether each stop has step free access and is accessible to all. Therefore there could be stops that are in the data which can’t be used. This is particularly relevant to our analysis as we produce breakdowns by disability.

  


**Population Weighted Centroids**

The OA Population Weighted Centroids are an official geographical product from the Office For National Statistics. These are used to determine where an Output Area can be allocated to, when aggregating statistics from OA to any other geographical level such as local authorities.

Centroids are calculated via the median centroid algorithm. The methodology is detailed [here](https://geoportal.statistics.gov.uk/documents/population-weighted-centroids-guidance/explore).

The population centroids are on an Output area basis. There are approximately 300 people living in an OA, therefore it automatically assumes that all 300 people live within this one point. As we apply a Eucliden buffer around the centroid (a circle with radius 500/1000M) there may not be an accessible path/pavement or road to get from the centroid to the stop that lies within the buffer.

The centroid might be in the middle of a lake or field and therefore not actually representative of where the population actually live.The centroids were calculated in 2011. There is a possibility that the areas in which people lived in have changed. For example if a new housing estate has been built since 2011, this may shift where the centroid lies.

  


**Urban/Rural Classification**

The definition of whether an OA is urban/rural can be found [here](https://www.ons.gov.uk/methodology/geography/geographicalproducts/ruralurbanclassifications/2011ruralurbanclassification). This is one definition of how to classify this. The EU uses a different definition and therefore there would be differences in the output statistics.

Urban/Rural classification uses a 2011 classification applied to the current OA boundaries. Classification of urban/rural might have changed since then. For example if a big housing estate was created in the countryside on the edge of a city, this might lead to that OA being classified as urban instead of rural.

  


**Disability Data**

The disability data used iis from the 2011 census, therefore a proportion is applied to population estimates for subsequent years. This method was suggested by the ONS Geo Spatial Department. These demographics might have changed since then.

The definition used for disability is the [GSS harmonized disability data](https://gss.civilservice.gov.uk/policy-store/measuring-disability-for-the-equality-act-2010/). The UN [definition](https://unstats.un.org/sdgs/metadata/files/Metadata-11-02-01.pdf) appears to be much wider for disability. This could lead to different output statistics when aggregating up.

**Annual Population Estimates**

These are the annual population estimates that ONS produce every year. They are an official statistic. These, unlike the census, are estimates and have a defined methodology [here](https://www.ons.gov.uk/peoplepopulationandcommunity/populationandmigration/populationestimates/methodologies/measuresofstatisticaluncertaintysummary). The methodology includes a section on statistical uncertainty about these numbers.

**LAD Boundaries**

There are several different types of geo spatial boundaries that can be used. (BFE) Best Full Extent is the boundary type we use. To be consistent with official statistics that ONS produce this is the type of boundary we use.

LA Boundaries are subject to change year on year. Big changes can occur such as merging of local authorities and name changes. Therefore comparisons between years need to be made carefully. There is a discussion on this point documented [here](https://github.com/ONSdigital/SDG_11.2.1/issues/152).

  
