BlueTopo
========

Overview
--------

Examples scripts for accessing and using BlueTopo.

Background
----------

https://www.nauticalcharts.noaa.gov/data/bluetopo.html

Requirements
------------

This codebase is written for Python 3 and relies on the following python
packages:

-   gdal / ogr
-   numpy
-   boto3

Installation
------------

Download the repo and use.  Alternatively, you may pip install and use the CLI. 

The two methods are described below. Additional changes to come to improve API to allow ease of use in other projects when time is available.

Method - Repo Quickstart
-------------

<ins>Setup</ins>:

Download the repo locally and confirm package requirements above are met

<ins>Usage</ins>:

To download the desired files, first create a geometry file (such as a geopackage) with a polygon depicting the area of interest.  Then run the following commands inside of a Python shell:
  
  > from nbs.bluetopo import fetch_tiles
  
  > fetch_tiles.main(r'C:\download_path', 'area_of_interest.gpkg')
  
To build a GDAL VRT of the downloaded tiles:

  > from nbs.bluetopo import build_vrt
  
  > build_vrt.main(r'C:\download_path')

Method - CLI Quickstart
-------------

<ins>Setup</ins>:

Download and install conda (If you have not already): [conda installation](https://docs.conda.io/projects/conda/en/latest/user-guide/install/)

Download and install git (If you have not already): [git installation](https://git-scm.com/book/en/v2/Getting-Started-Installing-Git)

Perform these commands in your terminal:

`conda create -n bluetopo_env -c conda-forge 'gdal>=3.4'`

`conda activate bluetopo_env`

`pip install git+https://github.com/noaa-ocs-hydrography/BlueTopo`

<ins>Usage</ins>:

Pass a directory path for your downloads and a geometry filepath to retrieve all available intersecting BlueTopo tiles

`fetch_tiles -d [DIRECTORY PATH] -g [GEOMETRY FILE PATH]`

Pass the same directory path to the build_vrt command to create a vrt from the retrieved tiles

`build_vrt -d [DIRECTORY PATH]`

Use `-h` for help and to see additional arguments. For straight forward usecases, the commands above are adequate.

Notes
-------

In addition to BlueTopo, modeling data is available. You may target modeling data using the target argument. The
primary difference between the two is the vertical datum. Modeling data is on MLLW.

Known Issues
-------

You may encounter issues if you access tiles in the S3 bucket while they are actively being updated by the NBS.

Authors
-------

-   Glen Rice (NOAA), <ocs.nbs@noaa.gov>

-   Tashi Geleg (Lynker / NOAA), <ocs.nbs@noaa.gov>


License
-------

This work, as a whole, falls under Creative Commons Zero (see
[LICENSE](LICENSE)).

Disclaimer
----------

This repository is a scientific product and is not official
communication of the National Oceanic and Atmospheric Administration, or
the United States Department of Commerce. All NOAA GitHub project code
is provided on an 'as is' basis and the user assumes responsibility for
its use. Any claims against the Department of Commerce or Department of
Commerce bureaus stemming from the use of this GitHub project will be
governed by all applicable Federal law. Any reference to specific
commercial products, processes, or services by service mark, trademark,
manufacturer, or otherwise, does not constitute or imply their
endorsement, recommendation or favoring by the Department of Commerce.
The Department of Commerce seal and logo, or the seal and logo of a DOC
bureau, shall not be used in any manner to imply endorsement of any
commercial product or activity by DOC or the United States Government.
