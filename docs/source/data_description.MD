**Data**

The Public Transport Availability project looks to assess the proportion of people who live near a public transport stop. Below is a description of the data sources used in order to perform this calculation.

**NaPTAN**

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

**Derived Variables**

The StopType that are included are in the calculations are "RSE", "RLY", "RPL", "TMU", "MET", "PLT", "BCE", "BST","BCQ", "BCS","BCT". After filtering there are 383,662 public transport access points.

A **capacity_type** variable is derived which classifies public transport as either high or low capacity. This is consistent with the UN [definition](https://unstats.un.org/sdgs/metadata/files/Metadata-11-02-01.pdf).

A **geometry** variable is derived which creates a polygon around each public transport access point. The polygon for a low capacity bus stop is a circle with radius of 500 metres with the access point being the centre point. The polygon for high capacity is the same with a circle with a radius of 1000 metres. These polygons will be used to determine if a weighted centroid lives within the polygon.

  
  
  


**Census Data**

The census takes place every 10 years and aims to obtain information on all households in the UK and statistics are published at various geographic levels. Output area (OA) is a lower level geography which contains on average approximately 300 people. For the purposes of our calculations each OA will be grouped together into one household.

Census data is used to calculate percentages of certain demographics which can then be applied to the annual population estimates. For example the annual population estimates do not include information on disability status. A proportion of people who are disabed can be calculated from the 2011 Census per OA and then applied to the population estimates data.

The [Population Weighted Centroids](https://data.gov.uk/dataset/5a08e622-1547-49ac-b626-d4f0d4067805/output-areas-december-2011-population-weighted-centroids) for OA from the 2011 census are used. These are OA’s containing where the midpoint of their population is. These are the points used to deduce whether an OA is contained within a service area.

The [Urban/Rural Classification](https://www.ons.gov.uk/methodology/geography/geographicalproducts/ruralurbanclassifications/2011ruralurbanclassification) is used to classify if an OA is urban or rural. This is used to be able to calculate different estimates for each classification. The OA’s are classed as ‘urban’ if they were allocated to a 2011 built-up area with a population of 10,000 people or more. All other remaining OAs are classed as ‘rural’.

The QS303UK - Long-term health problem or disability dataset, derived from the 2011 census, contains disability information on a OA basis. This information is transformed to be consistent with the [GSS harmonized disability data](https://gss.civilservice.gov.uk/policy-store/measuring-disability-for-the-equality-act-2010/). This allows us to produce estimates disaggregated by disability status.

  


**Population Estimates**

The ONS produces [population estimates](https://www.ons.gov.uk/peoplepopulationandcommunity/populationandmigration/populationestimates) every year. This contains information on population estimates for OA’s. This also contains splits of age and sex. These are annual so the year used is consistent with the calculation year.

  


**Local Authorities (LA) Boundary**

The boundaries of each [local authorities](https://data.gov.uk/dataset/51878530-7dd4-45df-b36b-9a0b01f3c136/local-authority-districts-december-2019-boundaries-uk-bgc) are used to ensure calculations are aggregated to an LA basis.

The[ lookup file ](https://geoportal.statistics.gov.uk/search?collection=Dataset&sort=name&tags=all(LUP_OA_WD_LAD))between LA and OA is important when aggregating OA estimates to produce the LA figure.

These boundaries and lookup files are annual so the year used is consistent with the calculation year.
