[![alt text](https://www.nauticalcharts.noaa.gov/data/images/bluetopo/logo.png)](https://www.nauticalcharts.noaa.gov/data/bluetopo.html)

---

<p align="center">
    <a href="#background">Background</a> •
    <a href="#requirements">Requirements</a> •
    <a href="#installation">Installation</a> •
    <a href="#quickstart">Quickstart</a> •
    <a href="#cli">CLI</a> •
    <a href="#notes">Notes</a> •
    <a href="#known-issues">Issues</a> •
    <a href="#authors">Contact</a>
</p>

## Overview
This project simplifies getting BlueTopo data in your area of interest.

## Background

[BlueTopo](https://www.nauticalcharts.noaa.gov/data/bluetopo.html) is a compilation of the best available public bathymetric data of U.S. waters.

Created by [NOAA Office of Coast Survey's](https://www.nauticalcharts.noaa.gov/) National Bathymetric Source project, [BlueTopo data](https://www.nauticalcharts.noaa.gov/data/bluetopo_specs.html) aims to provide depth information nationwide with the vertical uncertainty tied to that depth estimate as well as information on the survey source that it originated from. 

This data is presented in a multiband high resolution GeoTIFF with an associated raster attribute table. 

For answers to frequently asked questions, visit the [FAQ](https://www.nauticalcharts.noaa.gov/data/bluetopo_faq.html).

## Requirements

This codebase is written for Python 3 and relies on the following python
packages:

-   gdal / ogr
-   numpy
-   boto3

## Installation

Download and install conda (If you have not already): [conda installation](https://docs.conda.io/projects/conda/en/latest/user-guide/install/)

Download and install git (If you have not already): [git installation](https://git-scm.com/book/en/v2/Getting-Started-Installing-Git)

In the command line, create an environment with required packages

```
conda create -n bluetopo_env -c conda-forge 'gdal>=3.4'
```
```
conda activate bluetopo_env
```
```
pip install git+https://github.com/noaa-ocs-hydrography/BlueTopo
```

## Quickstart

To download the desired files, first create a geometry file (such as a geopackage) with a polygon depicting the area of interest.  Then run the following commands inside of a Python shell:

```python
from nbs.bluetopo import fetch_tiles
```
```python
fetch_tiles.main(r'C:\download_path', 'area_of_interest.gpkg')
```

To build a GDAL VRT of the downloaded tiles:
```python
from nbs.bluetopo import build_vrt
```
```python
build_vrt.main(r'C:\download_path')
```
## CLI

The CLI consists of two commands `fetch_tiles` and `build_vrt`. Confirm that you have the environment created in the installation guide activated.

In the command line, pass a directory path for your downloads and a geometry filepath to pull available intersecting BlueTopo tiles
```
fetch_tiles -d [DIRECTORY PATH] -g [GEOMETRY FILE PATH]
```
Pass the same directory path to the build_vrt command to create a VRT from the retrieved tiles
```
build_vrt -d [DIRECTORY PATH]
```
Use `-h` for help and to see additional arguments.

For most usecases, reusing the commands above to stay up to date in your area of interest is adequate.

## Notes

In addition to BlueTopo, modeling data is available. You may target modeling data using the target argument. The
primary difference between the two is the vertical datum. Modeling data is on a low water datum.

## Known Issues

You may encounter errors if you fetch tiles in the S3 bucket while they are actively being updated by the NBS.

## Authors

-   Glen Rice (NOAA), <ocs.nbs@noaa.gov>

-   Tashi Geleg (Lynker / NOAA), <ocs.nbs@noaa.gov>


## License

This work, as a whole, falls under Creative Commons Zero (see
[LICENSE](LICENSE)).

## Disclaimer

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
