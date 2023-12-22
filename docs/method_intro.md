# Calculation Methodology of Indicator 11.2.1 
Version 1.0

**James Westwood, Antonio Felton, Paige Hunter and Nathan Shaw.**


## Introduction

Our team has calculated data for transport accessibility across the UK following, as closely as we could, the method in the [UN Metadata for the SDG indicator 11.2.1](https://unstats.un.org/sdgs/metadata/?Text=&Goal=11&Target=11.2). 

The following is a write up of the methodology we employed to make these calculations. In this methodology writeup we aim to accurately reflect the method used in version 1.0 of our project; read more about versions below in the section “Project Versions”. 


## Acknowledgement

The main support for this project on both methodology and finding appropriate data sources has come from the geospatial department, in particular Musa Chirikeni, to whom we are very grateful. Other help has come from Michael Hodge who laid out the initial method we might approach in Python, and from the SDG data team who have been supportive data end users. We also thank Mark Simons who contributed to data management code in early 2022.


## Background  

The statistics calculated for this project are to be used as an indicator of the UK’s progress on target 11.2 of the Sustainable Development Goals (SDGs). The SDGs are a set of 17 Goals which 193 Member states signed up to in order to coordinate efforts on more sustainable and inclusive national development. The goals cover areas such as poverty, hunger, equality and the environment. Goal 11, which the indicator we are calculating for falls under, is concerned with Sustainable Cities. The data our team produces will be available on the [UK data platform for the SDG indicators](https://sdgdata.gov.uk/), specifically on[ this page](https://sdgdata.gov.uk/11-2-1/).

### Project history

The project was initiated in late 2019 by James Westwood with some initial help from the UK SDG data team and Michael Hodge from the Data Science Campus, who provided guidance on the tech that would need to be used and a rough step-by-step calculation process. 

### Project Versions

The project is managed using version control on the Github platform. One feature of the version controlling using Github is that we can make releases. At the time of writing we have not made any releases of the code, but when we feel that all features to calculate the statistics required by the end user have been written into our code, we will release version 1.0. 

Each version has a project board, wherein we group the issues describing the work that needs to be done to develop each feature required. [Project boards ](https://github.com/ONSdigital/SDG_11.2.1/projects?type=classic)are hosted on Github. 


<table>
  <tr>
   <td>Version
   </td>
   <td>Focus
   </td>
   <td>Example features
   </td>
  </tr>
  <tr>
   <td>1.0
   </td>
   <td><a href="https://github.com/ONSdigital/SDG_11.2.1/projects/1">Fully working reproducible calculation of transport availability across the UK</a>
   </td>
   <td>
<ul>

<li>Calculate the proportion of the population within 500m of a public transport access point for the whole of the UK.

<li>Disaggregate the above number by sex, age, disability status

<li>Be auditable and reproducable
</li>
</ul>
   </td>
  </tr>
  <tr>
   <td>1.1
   </td>
   <td><a href="https://github.com/ONSdigital/SDG_11.2.1/projects/3">Quality Assurance Phase</a>
   </td>
   <td>
<ul>

<li>Updates to code from feedback (e.g. from data team and topic expert)

<li>Data cleaning on import

<li>Validation on import

<li>Unit tests for functions
</li>
</ul>
   </td>
  </tr>
  <tr>
   <td>1.2
   </td>
   <td><a href="https://github.com/ONSdigital/SDG_11.2.1/projects/5">Enhanced functions and calculation</a>
   </td>
   <td>Focus on enhancements to the calculation
<p>
- Improve existing functions, making them more generic and robust
<p>
- Look into improving geographical accuracy with more granular calculations
<p>
- Improve the analysis with more disaggregations.
   </td>
  </tr>
  <tr>
   <td>1.3
   </td>
   <td><a href="https://github.com/ONSdigital/SDG_11.2.1/projects/4">Optimise Computation</a>
   </td>
   <td>Focus on enhancements to the functioning of the code:
<ul>

<li>Better data management (e.g. SQL tables)

<li>Refactoring code

<li>Code optimization (speed, memory) vectorised calculation, 
</li>
</ul>
   </td>
  </tr>
</table>







#### Shortcomings of the disaggregation method

Our team recognises that in our current method, ethnicity is not a disaggregation that we use. At this stage for version 1.0 we are attempting to output data called for by the methodology in the [UN Metadata](https://unstats.un.org/sdgs/metadata/?Text=&Goal=11&Target=11.2). We regret that this important disaggregation is not included however, so our team intend to include this additional disaggregation in version 1.1 as an enhancement above what the original methodology requires. Disaggregating on other protected characteristics, as well as deprivation levels may be considered too.


### Disability status


#### Classification and calculation of people with disabilities

We classify disability using data from the ONS UK census, which is consistent with GSS harmonized disability data. To understand the data, we looked at the questions and their possible responses in the[ Measuring disability for the Equality Act 2010 harmonisation guidance](https://gss.civilservice.gov.uk/policy-store/measuring-disability-for-the-equality-act-2010/).

The questions are as follows:


<table>
  <tr>
   <td><strong>Question</strong>
   </td>
   <td><strong>Response options</strong>
   </td>
  </tr>
  <tr>
   <td>Do you have any physical or mental health conditions or illnesses lasting or expected to last 12 months or more?
   </td>
   <td>Yes / No
   </td>
  </tr>
  <tr>
   <td>Does your condition or illness\do any of your conditions or illnesses reduce your ability to carry-out day-to-day activities?
   </td>
   <td>Yes, a lot / Yes, a little / No
   </td>
  </tr>
</table>


The guidance states that the persons meeting the following criteria include:


    "_A person who says yes, they have physical or mental health condition(s) or illness(es) lasting or expected to last for 12 months or more, but it doesn’t restrict their activity are non-disabled._"

Therefore in our calculation, people will be considered "Non-disabled" if they:



* answer no to the first question
* answer yes to the first question, but no to the second question.

And the calculations are as follows:

_Dtot = Dlot + Dlit_


I.e. "Total people with disabilities" = "Day-to-day activities limited a lot" + "Day-to-day activities limited a little"

Or, in our Python code


```
disab_total = disab_ltd_little + disab_ltd_lot
```


Where `disab_ltd_little` and `disab_ltd_lot` are each column or Pandas Series. 

Then the total of non disabled people given by

"Non-disabled" = "Total Population" - "Total people with disabilities"

Or in our Python code


```
non-disabled = pop_count - disab_total
```


#### Considerations on disability status

Our team has discussed options for counting individuals as either disabled or not. This is a complex and important area, and we recognise the importance of getting this as accurate as possible, as it may highlight areas in which those with disabilities are more affected by transport accessibility issues. 


##### Distance

Standard distances of 500m and 1km are applied as a radius around the transport access nodes in order to create the public transport service areas. We speculate that these distances likely do not represent an accessible distance for many people however- this might include wheelchair users, elderly people and families with young children. 


##### Definitions

We have opted to use a GSS Harmonised definition of disability for our analysis and the data comes from the census as described above. On the other hand the UN Metadata defines additional criteria to categorise public transport as conveniently accessible or not: 


    “Public transport accessible to all special-needs customers, including those who are physically, visually, and/or hearing-impaired, as well as those with temporary disabilities, the elderly, children and other people in vulnerable situations”_

In our analysis we are including the entire population, however, in our disaggregations we do not create a “special-needs” group. If we were to create such a group we should include people with temporary disabilities (if the data on this can be sourced), and the elderly or children. This has been proposed for version 1.2 of this project.

### Selection of age bins 

Population data was broken down by age on a year-by-year basis, from ages 0 through to 99. Rather than reporting the data or even calculating transport availability for every year in an age range, we opted to run the calculation for ages binned in 5-year brackets. 

We found no standard way to group ages. In other indicators across the SDG platform we found data grouped by age in various age increments and ranges, as seen in the [disaggregation report for age](https://sdgdata.gov.uk/sdg-data/disaggregation--age.html). We selected the 5-year brackets (0-4, 5-9, 10-14 etc) to be similar to other UK indicators such as [3.3.3](https://sdgdata.gov.uk/3-3-3/) and [3.4.1](https://sdgdata.gov.uk/3-4-1/) among others. However we realise there are many other indicators which are  not grouped in this way. 

If another age binning method is required, we plan to make our `age_binning` function more configurable (in version 1.2 of the project) so it will take population data by age and aggregate it into bins group by ages provided a `bin_size` parameter. This means that if the age binning needs to be changed so that, for instance, it can be compared to another dataset, the list of age groups can be changed easily and the analysis rerun. 

### Aggregation and reporting

To establish whether public stops and stations were within reach of people's places of residence the data analysis needed to be carried out at the most granular level possible, the output area level. However there are 175,434 output areas in England and Wales, so this would be too many data points to report on the data platform. Instead we aggregate up to large areas by request of our data end-user, the SDG data team. We aggregate our analysis up to the local authority (LA) level, and output areas fit perfectly within their parent local authority. 

The results aggregated in this way will display well on the [UK SDG data platform](https://sdgdata.gov.uk/), as it is well for developed to take this kind of geographical data! 
