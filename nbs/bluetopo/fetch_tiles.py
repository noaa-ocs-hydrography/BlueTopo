"""
fetch_tiles.py

0.0.1 20220614

glen.rice@noaa.gov 20220614

An example script for downloading BlueTopo datasets from AWS.

"""

import os
import boto3
from botocore import UNSIGNED
from botocore.client import Config
from osgeo import ogr


def download_bluetopo_tesselation(destination_directory: str, bucket: str = 'noaa-ocs-nationalbathymetry-pds',
                                  source_prefix: str = 'BlueTopo/BlueTopo-Tile-Scheme/BlueTopo_Tile_Scheme'):
    """ 
    Download the BlueTopo tesselation scheme geopackage from AWS.

    Parameters
    ----------
    destination_directory : destination path for the downloaded geometry file.
    bucket : AWS bucket for the National Bathymetric Source project.
    source_prefix : The prefix for the geopackage on AWS to find the file.

    Returns
    -------
    destination_name : the downloaded file path.
    """
    connection = {'aws_access_key_id': '', 'aws_secret_access_key': '', 'config': Config(signature_version=UNSIGNED)}
    client = boto3.client('s3', **connection)
    pageinator = client.get_paginator('list_objects_v2')
    geometry_object = pageinator.paginate(Bucket=bucket, Prefix=source_prefix).build_full_result()
    if len(geometry_object) == 0:
        print(f'No geometry found in {source_prefix}')
        return None
    source_name = geometry_object['Contents'][0]['Key']
    path, filename = os.path.split(source_name)
    if len(geometry_object) > 1:
        print(f'More than one geometry found in {source_prefix}, using {filename}')
    destination_name = os.path.join(destination_directory, source_name)
    if not os.path.exists(os.path.dirname(destination_name)):
        os.makedirs(os.path.dirname(destination_name))
    client.download_file(bucket, source_name, destination_name)
    print(f'downloaded {filename}')
    return destination_name


def download_bluetopo_tiles(download_tile_list: list, destination_directory: str,
                            bucket: str = 'noaa-ocs-nationalbathymetry-pds'):
    """
    Parameters
    ----------
    download_tile_list
    destination_directory
    bucket

    Returns
    -------
    tiles_not_found
    """
    # aws setup
    connection = {'aws_access_key_id': '', 'aws_secret_access_key': '', 'config': Config(signature_version=UNSIGNED)}
    client = boto3.client('s3', **connection)
    pageinator = client.get_paginator('list_objects_v2')
    # find and get files
    tiles_not_found = []
    for tilename in download_tile_list:
        source_prefix = f'BlueTopo/{tilename}/'
        destination_path = os.path.join(destination_directory, source_prefix)
        tile_files = pageinator.paginate(Bucket=bucket, Prefix=source_prefix).build_full_result()
        if len(tile_files) > 0:
            if not os.path.exists(os.path.dirname(destination_path)):
                os.makedirs(os.path.dirname(destination_path))
            for object_name in tile_files['Contents']:
                source_name = object_name['Key']
                destination_name = os.path.join(destination_directory, f'{source_name}')
                print(f'downloading {tilename}')
                client.download_file(bucket, source_name, destination_name)
        else:
            print(f'unable to find tile {tilename}')
            tiles_not_found.append(tilename)
    return tiles_not_found


def get_tile_list(desired_area_filename: str, tile_scheme_filename: str):
    """
    Parameters
    ----------
    desired_area_filename : str
        a gdal compatible file path denoting geometries that reflect the region of interest.
    tile_scheme_filename
        a gdal compatible file path denoting geometries that reflect the tesselation scheme with addressing information
        for the desired tiles.

    Returns
    -------
    [str] : list of tile addresses intersecting with the provided areas.
    """
    target = ogr.Open(desired_area_filename)
    if target is None:
        print('Unable to open desired area file')
        return None
    source = ogr.Open(tile_scheme_filename)
    if target is None:
        print('Unable to open desired area file')
        return None
    tile_names = []
    driver = ogr.GetDriverByName('MEMORY')
    intersection = driver.CreateDataSource('memData')
    intersect_lyr = intersection.CreateLayer('intersect_lyr', geom_type=ogr.wkbPolygon)
    source_layer = source.GetLayer(0)
    num_target_layers = target.GetLayerCount()
    for layer_num in range(num_target_layers):
        target_layer = target.GetLayer(layer_num)
        target_layer.Intersection(source_layer, intersect_lyr)
        for feature in intersect_lyr:
            tile_names.append(feature.GetField(0))
    return tile_names


def main(desired_area_filename: str, destination_path: str):
    """
    Parameters
    ----------
    desired_area_filename
    destination_path

    Returns
    -------
    None
    """
    if not os.path.exists(desired_area_filename):
        raise ValueError(f'The geometry file {desired_area_filename} for determining what to download does not exist.')
    if not os.path.exists(destination_path):
        os.makedirs(destination_path)
    bluetopo_geometry_filename = download_bluetopo_tesselation(destination_path)
    tile_list = get_tile_list(desired_area_filename, bluetopo_geometry_filename)
    download_bluetopo_tiles(tile_list, destination_path)
