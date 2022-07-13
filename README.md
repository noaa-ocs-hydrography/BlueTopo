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
Download the repo and use.  No streamlined (PIP) installation is available (yet).

Release Notes
-------------
To download the desired files, first create a geometry file (such as a geopackage) with a polygon depicting the area of interest.  Then run the following commands:
  
  > from nbs.bluetopo import fetch_tiles
  
  > fetch_tiles.main('area_of_interest.gpkg', r'C:\download_path')
  
To build a GDAL VRT of the downloaded tiles:

  > from nbs.bluetopo import build_vrt
  
  > build_vrt.main(r'C:\download_path')

Authors
-------

-   Glen Rice (NOAA), <ocs.nbs@noaa.gov>


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
