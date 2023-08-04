# Data

The Public Transport Availability project looks to assess the proportion of people who live near a public transport stop. Below is a description of the data sources used in order to perform this calculation. Most data sources are split into 3 sections: England & Wales, Scotland, and Northern Ireland (NI).

## NaPTAN

The National Public Transport Access Nodes (NaPTAN) dataset contains a list of all public transport access points in Great Britain including bus, rail and tram. This is open-source data and is publicly available. As of 3rd May 2022, the dataset includes around 435,000 public transport access points. The following columns are used within our calculations. The full schema for NapTAN can be found[ here](http://naptan.dft.gov.uk/naptan/schema/2.5/doc/NaPTANSchemaGuide-2.5-v0.67.pdf).

| **Column** | **Description**                                         |
| ---------- | ------------------------------------------------------- |
| NaptanCode | Seven- or eight-digit identifier for access point.      |
| CommonName | Name of bus stop.                                       |
| Easting    | Uk Geocoding reference.                                 |
| Northing   |                                                         |
| Status     | Whether an access point is active, inactive or pending. |
| StopType   | The type of access point e.g bus or rail                |


  The dataset is filtered based on two conditions.

1. The Status column must not be inactive. This ensures that historic public transport access points are not included in the calculations.
2. The StopType is either a bus or rail public transport access point.

![](https://lh4.googleusercontent.com/aWHDDVx3u12vC8HnbD395w61y_wIi-K7sZ38TkHJV2EqifGdOD8t5cc4E7fdIN1dApuK-CSaxcFYJ28Vxg6jN1varhbk8_PDPuNj8lLD4kwfXOlg-GX8fk4EeVjV58fHmXw9hFiCC9vQjKUjmeztDA)

## Derived Variables

The StopType that are included are in the calculations are "RSE", "RLY", "RPL", "TMU", "MET", "PLT", "BCE", "BST","BCQ", "BCS","BCT". After filtering there are 383,662 public transport access points.

A **capacity_type** variable is derived which classifies public transport as either high or low capacity. This is consistent with the UN [definition](https://unstats.un.org/sdgs/metadata/files/Metadata-11-02-01.pdf).

A **geometry** variable is derived which creates a polygon around each public transport access point. The polygon for a low capacity bus stop is a circle with radius of 500 metres with the access point being the centre point. The polygon for high capacity is the same with a circle with a radius of 1000 metres. These polygons will be used to determine if a weighted centroid lives within the polygon.

## Census Data

The census takes place every 10 years and aims to obtain information on all households in the UK and statistics are published at various geographic levels. Output area (OA) is a lower level geography which contains on average approximately 300 people. For the purposes of our calculations each OA will be grouped together into one household.

In Northern Ireland, [small areas](https://www.nisra.gov.uk/support/output-geography-census-2011/small-areas#:~:text=Small%20Areas%20were%20generally%20created,Electoral%20Wards%20in%20Northern%20Ireland.) are used as opposed to OAs. The use of OAs were replaced after the 2001 NI census and small areas have an approximate population of 400 people.

Census data is used to calculate percentages of certain demographics which can then be applied to the annual population estimates. For example the annual population estimates do not include information on disability status. A proportion of people who are disabed can be calculated from the 2011 Census per OA and then applied to the population estimates data.

### England & Wales

The [population weighted centroids](https://data.gov.uk/dataset/5a08e622-1547-49ac-b626-d4f0d4067805/output-areas-december-2011-population-weighted-centroids) for OA from the 2011 census are used. These are OA’s containing where the midpoint of their population is. These are the points used to deduce whether an OA is contained within a service area.

The [urban/rural classification](https://www.ons.gov.uk/methodology/geography/geographicalproducts/ruralurbanclassifications/2011ruralurbanclassification) is used to classify if an OA is urban or rural. This is used to be able to calculate different estimates for each classification. The OA’s are classed as ‘urban’ if they were allocated to a 2011 built-up area with a population of 10,000 people or more. All other remaining OAs are classed as ‘rural’.

The [QS303EW](https://www.nomisweb.co.uk/sources/census_2011_qs), a long-term health problem or disability dataset, derived from the 2011 census, contains disability information on an OA basis. This information is transformed to be consistent with the [GSS harmonized disability data](https://gss.civilservice.gov.uk/policy-store/measuring-disability-for-the-equality-act-2010/) and allows us to produce estimates disaggregated by disability status.

The [QS104EW](https://www.nomisweb.co.uk/census/2011/qs104ew) contains sex population estimates for each OA.


### Scotland

The [population weighted centroids](https://www.nrscotland.gov.uk/statistics-and-data/geography/our-products/census-datasets/2011-census/2011-boundaries) for each OA from the 2011 census are used. These are OA’s containing where the midpoint of their population is. These are the points used to deduce whether an OA is contained within a service area.

The [urban/rural classification](https://www.isdscotland.org/Products-and-Services/GPD-Support/Geography/Urban-Rural-Classification/) is used to classify if an OA is urban or rural. The UR6_2013_2014 column is used to determine if an output area is rural or urban. An OA is classified as 'urban' if they were allocated to a 2013/14 built-up area with a population of 10,000 people or more. All other remaining OAs are classified as 'rural'. This is consistent with the England & Wales definition.

The [QS303S](https://www.nomisweb.co.uk/sources/census_2011_qsuk), a long-term health problem or disability dataset, derived from the 2011 census, contains disability information on an OA basis. This information is transformed to be consistent with the [GSS harmonized disability data](https://gss.civilservice.gov.uk/policy-store/measuring-disability-for-the-equality-act-2010/) and allows us to produce estimates disaggregated by disability status.

### Northern Ireland

The [population weighted centroids](https://www.nisra.gov.uk/support/geography/output-areas-census-2001) used in Northern Ireland are based on the 2001 NI census' OAs as no further updates to the data have been given. Since the 2011 census, Northern Ireland use small areas which are slightly different to OAs. There is a 1-1 lookup table which converts each OA to a small area so that we can use the population weighted centroids from the 2001 census.

The [urban/rural classification](https://www.nisra.gov.uk/publications/settlement-2015-documentation) classifies a NI small area to be urban or rural. We classified an urban area to be where a settlement has a population greater than 10,000. The NISRA classify an urban area as greater than 5,000 population which is inconsistent with our classification. The previous definition was chosen to be consistent between England & Wales.

The [QS303NI](https://www.nisra.gov.uk/publications/2011-census-quick-statistics-tables-health), a long-term health problem or disability dataset, derived from the 2011 census, contains disability information on a small area basis. This information is transformed to be consistent with the [GSS harmonized disability data](https://gss.civilservice.gov.uk/policy-store/measuring-disability-for-the-equality-act-2010/) and allows us to produce estimates disaggregated by disability status.

## Annual Population Estimates

The ONS produces [population estimates](https://www.ons.gov.uk/peoplepopulationandcommunity/populationandmigration/populationestimates) every year for England & Wales. This contains information on population estimates for each OA. This also contains splits of age and sex for England & Wales **only**. These are annual so the year used is consistent with the calculation year.

We also use NI [population estimates](https://www.nisra.gov.uk/statistics/population/population-estimates-small-areas) from the Northern Ireland Statistics and Research Agency (NISRA) which give mid-year population estimates for small areas. These are not disaggregated by age and sex, so has to be calculated using proportions from 2011 census. This is explained in the calculation process.

Scotland currently has no mid-year population estimates for OA, and we can only calculate population for each OA based on LA level.


## Local Authorities (LA) Boundary Data

This section discusses the local authority boundaries data used for England & Wales, Scotland, and Northern Ireland.

### England & Wales
The boundaries of each [local authority](https://data.gov.uk/dataset/51878530-7dd4-45df-b36b-9a0b01f3c136/local-authority-districts-december-2019-boundaries-uk-bgc) are used to ensure calculations are aggregated to an LA basis.

This [lookup file](https://geoportal.statistics.gov.uk/search?collection=Dataset&sort=name&tags=all(LUP_OA_WD_LAD)) between LA and OA is used as it is important when aggregating OA estimates to a LA level for England & Wales.

These boundaries and lookup files are annual so the year used is consistent with the calculation year.

### Scotland

The boundaries of each [local authority](https://data.spatialhub.scot/dataset/local_authority_boundaries-is) for Scotland use 2021 boundaries only at the  moment. We are able to access a LA 2011 boundary but there is no lookup table for this so are using LA2021 boundary data and [lookup table](https://geoportal.statistics.gov.uk/datasets/ons::postcode-to-output-area-to-lower-layer-super-output-area-to-middle-layer-super-output-area-to-local-authority-district-november-2021-lookup-in-the-uk/about). Future work will look to investigating the differences between LA 2011 boundaries and LA 2021 boundaries.

Unfortunately, Scotland boundaries and lookup files are not annual so we cannot access more accurate data for other years.

### Northern Ireland

The same local authorities boundary file that is used in England & Wales is also used for Northern Ireland.

We used an OA to SA lookup file, as well as an SA to LA lookup file. This was due to only having population weighted centroids for each OA in 2011, meaning we converted each OA to an SA using a 1:1 lookup table. This meant we could then use an SA to LA lookup table to aggregate the SA estimates to LA level.
