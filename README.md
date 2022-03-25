# The Public Transport Accessibility Project 
## Analysis for SDG Indicator 11.2.1

This project is to build a data pipeline to analyse data for the UN Sustainable Development Goal indicator 11.2.1, which is part of goal 11:

> "Make cities and human settlements inclusive, safe, resilient and sustainable"

By assessing geographical and census data with data on public transport access points (stops and stations), an assessment can be made about the degree of acessability individuals in the population have to public transport. 


## Description of Target 11.2
By 2030, provide access to safe, affordable, accessible and sustainable transport systems for all, improving road safety, notably by expanding public transport, with special attention to the needs of those in vulnerable situations, women, children, persons with disabilities and older persons 


## Description of Indicator 11.2.1
"Proportion of population that has convenient access to public transport, by sex, age and persons with disabilities"


## Aims of the project

The aims are:

* to build a data pipeline that can analyse data to assess the availability of public transport services for the population of the UK
* to make the code reusable so that it may be able to analyse other data and:
    * assess the availability of other services across the UK
    * be used to assess the availability of services in other nations

## Process

![step1](https://github.com/james-westwood/SDG_11.2.1/raw/master/img_readme/11-2-1-process-step1.jpg)

![step2](https://github.com/james-westwood/SDG_11.2.1/raw/master/img_readme/11-2-1-process-step2.jpg)

![step3](https://github.com/james-westwood/SDG_11.2.1/raw/master/img_readme/11-2-1-process-step3.jpg)


## Deliverables of the project

1. Open source reuseable code which relies only on open source resources
2. A new dataset with analysis of public transport availability for the UK population
3. Three additional datasets with analysis of public transport availability disagregated by sex, age and disabilities

## Next steps in development

- isolate the smallest areas available on the census dataset
- use the same method of creating a centroid and then a 5km buffer around those areas
- for every area, run a points in polygons enquiry to answer 'how many stops in the buffer?'
- generate a small dataset with columns `['area_name','population','stops_within_5km']
- create statistical pipeline for analysis

## Resources

- https://www.efgs.info/wp-content/uploads/2019/11/5a3_HugoPoelman.pdf

# Running the script

## Requirements: 

A number of problems with dependencies have been experienced while developing this, so it is strongly recommended that you use a virtual environment (either conda or venv) and use the provided requirements.txt to install the needed versions of packages.

### Preliminaries
Before starting, please ensure that [Anaconda3](https://docs.anaconda.com/anaconda/install/index.html) and [Git](https://git-scm.com/book/en/v2/Getting-Started-Installing-Git) are installed. 

### Cloning the repository


### Create an environment 

Here creating an environment called "SDG_11.2.1" with the version of Python that this was developed in

    conda create --name SDG_11.2.1 python=3.8

### Activate the environment

First go to the project directory 

    $ cd project-directory
    
Then activate the environment

    $ conda activate SDG_11.2.1
    
Or on Windows this would be

   $ activate SDG_11.2.1

Then you should see the environment name in brackets before the prompt, similar to:

    (SDG_11.2.1) $

Make sure you are using the correct Python interpreter by checking your Python path:

In Linux:

    $ which python

And in Windows use the following command in cmd or Anaconda prompt:

    $ where python

Which should return something like:

C:\Python36\envs\SDG_11.2.1\python.exe

Showing your are using the Python from the virtual environment, not the base installation of Python. 

### Create the environment from the yaml file

Run the following to get the environment set up 

`conda env create --file=environment.yml`

and 

`conda activate SDG_11.2.1` 

or on Windows

`activate SDG_11.2.1` 

to activate it. 

Note: On on Linux Ubuntu/Mint 18.04 you may have to install rtree from apt instead of pip. Run: 

    (SDG_11.2.1) $ sudo apt install python3-rtree



