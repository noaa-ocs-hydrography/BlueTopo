"""
fetch_tiles.py

0.0.1 20220614

glen.rice@noaa.gov 20220614

An example script for downloading BlueTopo datasets from AWS.

"""

import os, datetime
import boto3
from botocore import UNSIGNED
from botocore.client import Config
from osgeo import ogr, osr


def download_bluetopo_tesselation(destination_directory: str, bucket: str = 'noaa-ocs-nationalbathymetry-pds',
                                  source_prefix: str = 'BlueTopo/BlueTopo-Tile-Scheme/BlueTopo_Tile_Scheme',
                                  ) -> str:
    """ 
    Download the BlueTopo tesselation scheme geopackage from AWS.

    Parameters
    ----------
    destination_directory : str
        destination path for the downloaded geometry file.
    bucket : str
        AWS bucket for the National Bathymetric Source project.
    source_prefix : str
        The prefix for the geopackage on AWS to find the file.

    Returns
    -------
    destination_name : the downloaded file path string.
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
                            bucket: str = 'noaa-ocs-nationalbathymetry-pds') -> [[str],[str]]:
    """
    Parameters
    ----------
    download_tile_list :
        a list of the desired tiles by name corresponding to the names in the aws bucket.
    destination_directory : str
        destination directory for the downloaded geometry file.
    bucket : str
        name of the source aws bucket.  Defaults to the National Bathymetry bucket.

    Returns
    -------
    [[list of tiles found],[list of tiles not found]]
    """
    # aws setup
    connection = {'aws_access_key_id': '', 'aws_secret_access_key': '', 'config': Config(signature_version=UNSIGNED)}
    client = boto3.client('s3', **connection)
    pageinator = client.get_paginator('list_objects_v2')
    # find and get files
    tiles_found = []
    tiles_not_found = []
    display_count = 1
    msg = ''
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
                remove_previous = len(msg) * '\b'
                msg = f'downloading{display_count * "."}'
                print(remove_previous + msg, end='')
                display_count += 1
                client.download_file(bucket, source_name, destination_name)
            tiles_found.append(tilename)
        else:
            # print(f'unable to find tile {tilename}')
            tiles_not_found.append(tilename)
        if display_count > 10:
            display_count = 1
    return tiles_found, tiles_not_found


def get_tile_list(desired_area_filename: str, tile_scheme_filename: str, tile_name_field: str = 'tile') -> [str]:
    """
    Parameters
    ----------
    desired_area_filename : str
        a gdal compatible file path denoting geometries that reflect the region of interest.
    tile_scheme_filename : str
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
    source_crs = source_layer.GetSpatialRef()
    num_target_layers = target.GetLayerCount()
    for layer_num in range(num_target_layers):
        target_layer = target.GetLayer(layer_num)
        target_crs = target_layer.GetSpatialRef()
        same_crs = target_crs.IsSame(source_crs)
        if not same_crs:
            transformed_input = transform_layer(target_layer, source_crs)
            target_layer = None
            target_layer = transformed_input.GetLayer(0)
        target_layer.Intersection(source_layer, intersect_lyr)
        if not same_crs:
            transformed_input = None
        intersect_lyr_defn = intersect_lyr.GetLayerDefn()
        for field_num in range(intersect_lyr_defn.GetFieldCount()):
            fdefn = intersect_lyr_defn.GetFieldDefn(field_num)
            if fdefn.name == tile_name_field:
                break
        for feature in intersect_lyr:
            tile_names.append(feature.GetField(field_num))
    return tile_names

def transform_layer(input_layer, desired_crs) -> ogr.Layer:
    """
    Transform a provided ogr layer to the provide osr coordinate reference system.

    Parameters
    ----------
    input_layer : ogr.Layer
        the ogr layer to be transformed
    desired_crs : osr object
        the target coordinate system for the transform
    Returns
    -------
    transformed ogr layer
    """
    target_crs = input_layer.GetSpatialRef()
    coordTrans = osr.CoordinateTransformation(target_crs, desired_crs)

    driver = ogr.GetDriverByName('MEMORY')
    transformed_input = driver.CreateDataSource('memData')
    transformed_lyr = transformed_input.CreateLayer('transformed_lyr', geom_type=input_layer.GetGeomType())
    outLayerDefn = transformed_lyr.GetLayerDefn()
    inFeature = input_layer.GetNextFeature()
    transformed_features = 0
    while inFeature:
        geom = inFeature.GetGeometryRef()
        geom.Transform(coordTrans)
        outFeature = ogr.Feature(outLayerDefn)
        outFeature.SetGeometry(geom)
        transformed_lyr.CreateFeature(outFeature)
        outFeature = None
        inFeature = input_layer.GetNextFeature()
        transformed_features += 1
    # print(f'Transformed {transformed_features} to support comparison between source tiles and desired crs')
    return transformed_input

def main(desired_area_filename: str, destination_path: str) -> [[str],[str]]:
    """
    Parameters
    ----------
    desired_area_filename : str
        a gdal compatible file path denoting geometries that reflect the region of interest.
    destination_path : str
        destination path for the downloaded geometry file.

    Returns
    -------
    [[list of tiles found],[list of tiles not found]]
    """
    start = datetime.datetime.now()
    if not os.path.exists(desired_area_filename):
        raise ValueError(f'The geometry file {desired_area_filename} for determining what to download does not exist.')
    if not os.path.exists(destination_path):
        os.makedirs(destination_path)
    bluetopo_geometry_filename = download_bluetopo_tesselation(destination_path)
    tile_list = get_tile_list(desired_area_filename, bluetopo_geometry_filename)
    tiles_found, tiles_not_found = download_bluetopo_tiles(tile_list, destination_path)
    done = datetime.datetime.now()
    lapse = done - start
    print(f'\nOperation complete after {lapse} with {len(tiles_found)} tiles downloaded and {len(tiles_not_found)} '
          f'not available on AWS.')
    return tiles_found, tiles_not_found
