# Project: Analysis_SDG_11.2.1

This is project to build a data pipeline to analyse data for the UN Sustainable Development Goal 11.2.1

## Description of 11.2
By 2030, provide access to safe, affordable, accessible and sustainable transport systems for all, improving road safety, notably by expanding public transport, with special attention to the needs of those in vulnerable situations, women, children, persons with disabilities and older persons 


## Description of Indicator 11.2.1
"Proportion of population that has convenient access to public transport, by sex, age and persons with disabilities"


## Aims of the project

The aims are:

* to build a data pipeline that can analyse data to assess the availability of public transport services for the population of the UK
* to make the code reusable so that it may be able to analyse other data and:
    * assess the availability of other services across the UK
    * be used to assess the availability of services in other nations


## Requirements: 

A number of problems with dependencies have been experienced while developing this, so it is strongly recommended that you use a virtual environment (either conda or venv) and use the provided requirements.txt to install the needed versions of packages.


### Create an environment

Here creating an environment called "SDG_11.2.1" with the version of Python that this was developed in

    conda create --name SDG_11.2.1 python=3.8

### Activate the environment

First go to the project directory 

    $ cd project-directory
    
Then activate the environment

    $ conda activate SDG_11.2.1

Then you should see the environment name in brackets before the prompt, similar to:

    (SDG_11.2.1) $
    
### Install the dependencies

You should be in correct directory, where `requirements.txt` exists. Run:

    (SDG_11.2.1) $ pip install -r requirements.txt


Note: On on Linux Ubuntu/Mint 18.04 you may have to install rtree from apt instead of pip. Run: 

    (SDG_11.2.1) $ sudo apt install python3-rtree

### Development Method

1. get public transport location data - NAPTAN a) Clean it if necessary
2. get population location data -  LSOA from ONS
3. use Fiona to read location data
4. limit to one or two locations, e.g. London and a more rural area
5. draw Euclidean Buffers around LSOA polygon centre points
6. find number of public transport stops in the polygon with “points in polygons” approach
