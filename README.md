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

## Resources

- https://www.efgs.info/wp-content/uploads/2019/11/5a3_HugoPoelman.pdf

# For Developers

## Requirements: 

A number of problems with dependencies have been experienced while developing this, so it is strongly recommended that you use a virtual environment (either conda or venv) and use the provided requirements.txt to install the needed versions of packages. 

**Note for ONS staff:** It is unlikely that you will be able to install all the needed dependencies to run this script, therefore it is recommended that your devlopment work is carried out on an off-network computer.

Before starting this process, please ensure that [Anaconda3](https://docs.anaconda.com/anaconda/install/index.html) and [Git](https://git-scm.com/book/en/v2/Getting-Started-Installing-Git) are installed. We recommend running the script using VSCode, as this is what we use in these instructions, and is downloadable [here](https://code.visualstudio.com/download).

If you are using Windows, you will have to add the conda and python path into your Windows Path environment. For guidance, follow the tutorial [here](https://www.datacamp.com/community/tutorials/installing-anaconda-windows).

### Cloning the repository
The first step is setting up your SSH key for GitHub. The process will slightly vary depending on what OS you are running from. Here are useful tutorials for [Windows 10](https://medium.com/devops-with-valentine/2021-how-to-set-up-your-ssh-key-for-github-on-windows-10-afe6e729a3c0) or [Mac and Linux](https://www.atlassian.com/git/tutorials/git-ssh).

You should now have your SSH key set up. To clone the repository, we need to first go to the project directory (where you would like it saved on your local drive).

    $ cd project-directory
Then activate use the SSH address to clone the repository 

    $ git clone SSH_address

You can then open the folder SDG_11.2.1 within VSCode using "Open Folder" in Source Control.

### Create an environment 

Virtual environments are extremely useful when working on different projects as they can be set up in a way to only have packages installed in that environment and not globally - it does not affect base Python installation in any way.

Create an environment called "SDG_11.2.1" with the version of Python that this was developed in

    conda create --name SDG_11.2.1 python=3.8

**Troubleshooting:** When creating the environment on Windows, it might fail with a HTTP error when trying to fetch package metadata. Following [these steps](https://stackoverflow.com/questions/50125472/issues-with-installing-python-libraries-on-windows-condahttperror-http-000-co/62483686#62483686) should resolve the issue.

### Activate the environment

First go to the project directory/wherever you have saved to on your local drive. On Windows this may look like:

    $ cd C:\Users\name\project-directory
    
Then activate the environment

    $ conda activate SDG_11.2.1

The above command will **not work for Windows** unless you have the Conda path linked to your Windows path environment (see [this tutorial](https://www.datacamp.com/community/tutorials/installing-anaconda-windows)).

If you do not have the Conda path linked, you can use the following for Windows:

    $ activate SDG_11.2.1

Then you should see the environment name in brackets before the prompt, similar to:

    (SDG_11.2.1) $

**Troubleshooting:** Activating the environment in the Powershell terminal does not work for Windows. Set the default terminal to Command Prompt and activate from here.

1. Press CTRL+Shift+P and search for 'Terminal: Configure Terminal Settings'.
2. Under the 'Terminal > External: Windows Exec' section enter the path to your cmd. E.g.

        C:\WINDOWS\System32\cmd.exe

Make sure you are using the correct Python interpreter by checking your Python path:

In Linux:

    $ which python

And in Windows use the following command in cmd or Anaconda prompt:

    $ where python

Which should return something like:

C:\Python36\envs\SDG_11.2.1\python.exe

Showing your are using the Python from the virtual environment, not the base installation of Python.

### Installing dependencies
First, ensure you are in the project directory

    $ cd C:\Users\name\project-directory

To install the requirements write

    conda install --file requirements.txt

This may throw an error if you do not have all the packages required

    PackageNotFoundError: The following packages are not available from current channels

If this does, write the following with the package names that the error has shown you are missing:

    pip install packagename

The script should now be set up to use.

### Setting Git configuration

To be able to contribute to the project via Git, you will need to add the email and user name associated to your account to the config file

    git config --global user.email "email"
    
    git config --global user.name "username"