"""
fetch_tiles.py

0.0.1 20220614

glen.rice@noaa.gov 20220614

An example script for downloading BlueTopo datasets from AWS.

"""

import sys
import boto3
import datetime
import numpy as np
import os
import sqlite3
from botocore import UNSIGNED
from botocore.client import Config
from osgeo import ogr, osr, gdal
from nbs.bluetopo.build_vrt import connect_to_survey_registry

def download_bluetopo_tesselation(registry_connection: sqlite3.Connection, destination_directory: str, 
                                  bucket: str = 'noaa-ocs-nationalbathymetry-pds',
                                  source_prefix: str = 'BlueTopo/_BlueTopo_Tile_Scheme/BlueTopo_Tile_Scheme',
                                  ) -> str:
    """ 
    Download the BlueTopo tesselation scheme geopackage from AWS.

    Parameters
    ----------
    registry_connection
        A database connection object
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
    # delete existing tilescheme file
    cursor = registry_connection.cursor()
    cursor.row_factory = sqlite3.Row
    cursor.execute("SELECT * FROM tileset")
    for tilescheme in [dict(row) for row in cursor.fetchall()]:
        try:
            os.remove(os.path.join(destination_directory, tilescheme['location']))
        except (OSError, PermissionError):
            continue
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
    # replace tilescheme record
    cursor.execute('REPLACE INTO tileset(tilescheme, location, downloaded) VALUES(?, ?, ?)',
                  ('BlueTopo', source_name, datetime.datetime.now()))
    registry_connection.commit()
    return destination_name


def download_bluetopo_tiles(registry_connection: sqlite3.Connection, destination_directory: str,
                            bucket: str = 'noaa-ocs-nationalbathymetry-pds') -> [[str],[str],[str]]:
    """
    Parameters
    ----------
    registry_connection
        A database connection object
    destination_directory : str
        destination directory for the downloaded geometry file.
    bucket : str
        name of the source aws bucket.  Defaults to the National Bathymetry bucket.
    Returns
    -------
    [[list of existing tiles],[list of tiles found],[list of tiles not found]]
    """
    # all db tiles
    download_tile_list = all_db_tiles(registry_connection)
    # not downloaded tiles
    new_tile_list = [download_tile for download_tile in download_tile_list if download_tile['geotiff_disk'] is None or
                    download_tile['rat_disk'] is None]
    print(f'{len(new_tile_list)} new tiles being downloaded')
    # aws setup
    connection = {'aws_access_key_id': '', 'aws_secret_access_key': '', 'config': Config(signature_version=UNSIGNED)}
    client = boto3.client('s3', **connection)
    pageinator = client.get_paginator('list_objects_v2')
    # find and get files
    existing_tiles = []
    tiles_found = []
    tiles_not_found = []
    display_count = 1
    msg = ''
    for tile_fields in download_tile_list:
        if (tile_fields['geotiff_disk'] and tile_fields['rat_disk'] 
        and os.path.isfile(os.path.join(destination_directory, tile_fields['geotiff_disk'])) 
        and os.path.isfile(os.path.join(destination_directory, tile_fields['rat_disk']))):
            existing_tiles.append(tile_fields['tilename'])
            continue
        tilename = tile_fields['tilename']
        source_prefix = f'BlueTopo/{tilename}/'
        tile_files = pageinator.paginate(Bucket=bucket, Prefix=source_prefix).build_full_result()
        if len(tile_files) > 0:
            for object_name in tile_files['Contents']:
                source_name = object_name['Key']
                output_dest = os.path.join('BlueTopo', f"UTM{tile_fields['utm']}", os.path.basename(source_name))
                destination_name = os.path.join(destination_directory, output_dest)
                if not os.path.exists(os.path.dirname(destination_name)):
                    os.makedirs(os.path.dirname(destination_name))
                remove_previous = len(msg) * '\b'
                msg = f'downloading{display_count * "."}'
                print(remove_previous + msg, end='')
                display_count += 1
                client.download_file(bucket, source_name, destination_name)
                if '.aux' in destination_name.lower():
                    tile_fields['rat_disk'] = output_dest
                else:
                    tile_fields['geotiff_disk'] = output_dest
            update_records(registry_connection, tile_fields)
            tiles_found.append(tilename)
        else:
            # print(f'unable to find tile {tilename}')
            tiles_not_found.append(tilename)
        if display_count > 10:
            display_count = 1
    return existing_tiles, tiles_found, tiles_not_found


def get_tile_list(desired_area_filename: str, tile_scheme_filename: str) -> [str]:
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
    if source is None:
        print('Unable to open tile scheme file')
        return None
    driver = ogr.GetDriverByName('MEMORY')
    intersection = driver.CreateDataSource('memData')
    intersect_lyr = intersection.CreateLayer('intersect_lyr', geom_type=ogr.wkbPolygon)
    source_layer = source.GetLayer(0)
    source_crs = source_layer.GetSpatialRef()
    num_target_layers = target.GetLayerCount()
    feature_list = []
    for layer_num in range(num_target_layers):
        target_layer = target.GetLayer(layer_num)
        target_crs = target_layer.GetSpatialRef()
        same_crs = target_crs.IsSame(source_crs)
        if not same_crs:
            transformed_input = transform_layer(target_layer, source_crs)
            target_layer = transformed_input.GetLayer(0)
        target_layer.Intersection(source_layer, intersect_lyr)
        if not same_crs:
            transformed_input = None
        intersect_lyr_defn = intersect_lyr.GetLayerDefn()
        for feature in intersect_lyr:
            field_list = {}
            for field_num in range(intersect_lyr_defn.GetFieldCount()):
                field_list[intersect_lyr_defn.GetFieldDefn(field_num).name] = feature.GetField(field_num)
            feature_list.append(field_list)
    return feature_list

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
    coord_trans = osr.CoordinateTransformation(target_crs, desired_crs)
    driver = ogr.GetDriverByName('MEMORY')
    transformed_input = driver.CreateDataSource('memData')
    transformed_lyr = transformed_input.CreateLayer('transformed_lyr', geom_type=input_layer.GetGeomType())
    outlayer_defn = transformed_lyr.GetLayerDefn()
    in_feature = input_layer.GetNextFeature()
    transformed_features = 0
    while in_feature:
        geom = in_feature.GetGeometryRef()
        geom.Transform(coord_trans)
        out_feature = ogr.Feature(outlayer_defn)
        out_feature.SetGeometry(geom)
        transformed_lyr.CreateFeature(out_feature)
        out_feature = None
        in_feature = input_layer.GetNextFeature()
        transformed_features += 1
    # print(f'Transformed {transformed_features} to support comparison between source tiles and desired crs')
    return transformed_input

def update_records(registry_connection: sqlite3.Connection, tile: dict) -> None:
    """
    Parameters
    ----------
    registry_connection
        A database connection object
    tile : dict
        dictionary containing fields for a tile
    Returns
    -------
    None
    """
    cursor = registry_connection.cursor()
    cursor.execute('UPDATE tiles SET geotiff_disk = ?, rat_disk = ? where tilename = ?', 
                  (tile['geotiff_disk'], tile['rat_disk'],tile['tilename'],))
    cursor.execute('''INSERT INTO vrt_subregion(region, utm, res_2_vrt, res_2_ovr, res_4_vrt, res_4_ovr, res_8_vrt, 
                                                res_8_ovr, complete_vrt, complete_ovr, built)
                                                VALUES(?, ?, ?, ?, ? ,? , ?, ? ,? ,? ,?)
                                                ON CONFLICT(region) DO UPDATE
                                                SET utm = EXCLUDED.utm,
                                                res_2_vrt = EXCLUDED.res_2_vrt,
                                                res_2_ovr = EXCLUDED.res_2_ovr,
                                                res_4_vrt = EXCLUDED.res_4_vrt,
                                                res_4_ovr = EXCLUDED.res_4_ovr,
                                                res_8_vrt = EXCLUDED.res_8_vrt,
                                                res_8_ovr = EXCLUDED.res_8_ovr,
                                                complete_vrt = EXCLUDED.complete_vrt,
                                                complete_ovr = EXCLUDED.complete_ovr,
                                                built = EXCLUDED.built''',
                  (tile['subregion'], tile['utm'], None, None, None, None, None, None, None, None, 0))
    cursor.execute('''INSERT INTO vrt_utm(utm, utm_vrt, utm_ovr, built)
                                                VALUES(?, ?, ?, ?)
                                                ON CONFLICT(utm) DO UPDATE
                                                SET utm_vrt = EXCLUDED.utm_vrt,
                                                utm_ovr = EXCLUDED.utm_ovr,
                                                built = EXCLUDED.built''',
                  (tile['utm'], None, None, 0))
    registry_connection.commit()

def insert_new(registry_connection: sqlite3.Connection, tiles: dict) -> int:
    """
    Parameters
    ----------
    registry_connection
        A database connection object
    tiles : list
        list of dictionaries containing a tile's fields
    Returns
    -------
    amount of delivered tiles with links in given tile list
    """
    cursor = registry_connection.cursor()
    delivered_tiles = [(tile['tile'],) for tile in tiles if tile['Delivered_Date'] 
                        and tile['GeoTIFF_Link'] and tile['RAT_Link']]
    cursor.executemany('INSERT INTO tiles(tilename) VALUES(?) ON CONFLICT DO NOTHING', delivered_tiles)
    registry_connection.commit()
    return len(delivered_tiles)

def all_db_tiles(registry_connection: sqlite3.Connection) -> list:
    """
    Parameters
    ----------
    registry_connection
        A database connection object
    Returns
    -------
    list of all tiles in database as dictionaries containing the tile's fields
    """
    cursor = registry_connection.cursor()
    cursor.row_factory = sqlite3.Row
    cursor.execute("SELECT * FROM tiles")
    return [dict(row) for row in cursor.fetchall()]

def upsert_tiles(tile_scheme: str, bluetopo_path: str, registry_connection: sqlite3.Connection) -> None:
    """
    Parameters
    ----------
    tile_scheme
        path of the BlueTopo tilescheme file
    bluetopo_path
        destination directory for project's output
    registry_connection
        A database connection object
    Returns
    -------
    None
    """
    cursor = registry_connection.cursor()
    # tiles in registry
    db_tiles = all_db_tiles(registry_connection)
    # same tiles in tilescheme
    bluetopo_ds = ogr.Open(tile_scheme)
    bluetopo_lyr = bluetopo_ds.GetLayer()
    lyr_def = bluetopo_lyr.GetLayerDefn()
    bt_tiles = []
    for ft in bluetopo_lyr:
        field_list = {}
        for field_num in range(lyr_def.GetFieldCount()):
            geom = ft.GetGeometryRef()
            wkt_geom = geom.ExportToWkt()
            field_name = lyr_def.GetFieldDefn(field_num).name
            field_list[field_name.lower()] = ft.GetField(field_name)
            field_list['wkt_geom'] = wkt_geom
        bt_tiles.append(field_list)
    # polygons that depict regions
    global_tileset = global_region_tileset(1, '1.2')
    gs = ogr.Open(global_tileset)
    gs_lyr = gs.GetLayer().GetName()
    insert_tiles = []
    for db_tile in db_tiles:
        bt_tile = [bt_tile for bt_tile in bt_tiles if db_tile['tilename'] == bt_tile['tile']]
        if len(bt_tile) == 0:
            print(f"Warning: {db_tile['tilename']} in database appears to have been removed from latest BlueTopo tilescheme")
            continue
        if len(bt_tile) > 1:
            raise ValueError(f"More than one tilename {db_tile['tilename']} found in tileset. Please alert NBS.")
        # proper behavior for following edgecase? 
        # inserted only when delivered exists so indicates delivered date removed post-insertion to None.
        if bt_tile[0]['delivered_date'] is None:
            print(f"Warning: Unexpected removal of delivered date for tile {db_tile['tilename']}")
            continue
        if db_tile['delivered_date'] is None or bt_tile[0]['delivered_date'] > db_tile['delivered_date']:
            try:
                if db_tile['geotiff_disk'] and os.path.isfile(os.path.join(bluetopo_path, db_tile['geotiff_disk'])):
                    os.remove(os.path.join(bluetopo_path, db_tile['geotiff_disk']))
                if db_tile['rat_disk'] and os.path.isfile(os.path.join(bluetopo_path, db_tile['rat_disk'])):
                    os.remove(os.path.join(bluetopo_path, db_tile['rat_disk']))
            except (OSError, PermissionError) as e:
                print(f'failed to remove older files for tile {db_tile["tilename"]} \nplease close all files and attempt fetch again')
                gdal.Unlink(global_tileset)
                raise e
            # retrieve region for tile
            sql_statement = f'select * from {gs_lyr} where ST_Intersects(ST_GeomFromText("{bt_tile[0]["wkt_geom"]}", 4326), geom)'
            lyr = gs.ExecuteSQL(sql_statement)
            if lyr.GetFeatureCount() != 1:
                intersected_regions = [feat.GetField('Region') for feat in lyr]
                debug = f"""
                        debug:
                        Error getting subregion for {db_tile['tilename']}.
                        Intersected subregions {intersected_regions}.
                        {lyr.GetFeatureCount()} subregion(s).
                        Python {sys.version_info.major}.{sys.version_info.minor}.{sys.version_info.micro} 
                        GDAL {gdal.VersionInfo()} 
                        SQLite {sqlite3.sqlite_version}
                        Tileset count {gs.GetLayer().GetFeatureCount()}
                        Tile {bt_tile[0]}
                        {sql_statement}
                        """
                try:
                    lyr_alt = gs.GetLayer()
                    lyr_alt.SetSpatialFilter(ogr.CreateGeometryFromWkt(bt_tile[0]["wkt_geom"]))
                except:
                    raise ValueError(debug + '\nGeometry Failure')
                if lyr_alt.GetFeatureCount() == 1:
                    lyr = lyr_alt
                else:
                    if not os.path.exists(os.path.join(bluetopo_path, 'Debug')):
                        os.makedirs(os.path.join(bluetopo_path, 'Debug'))
                    ogr.GetDriverByName('GPKG').CopyDataSource(gs, os.path.join(bluetopo_path, "Debug", f"GS_Debug.gpkg"))
                    gdal.Unlink(global_tileset)
                    raise ValueError(debug + '\nExported debug tileset')
            region_ft = lyr.GetNextFeature()
            bt_tile[0]['region'] = region_ft.GetField('Region')
            insert_tiles.append((bt_tile[0]['tile'], bt_tile[0]['geotiff_link'],
                                                        bt_tile[0]['rat_link'], bt_tile[0]['delivered_date'], 
                                                        bt_tile[0]['resolution'], bt_tile[0]['utm'], 
                                                        bt_tile[0]['region'],))
    if insert_tiles:
        cursor.executemany('''INSERT INTO tiles(tilename, geotiff_link, rat_link, 
                                        delivered_date, resolution, utm, subregion)
                                                    VALUES(?, ?, ? ,? ,? ,?, ?)
                                                    ON CONFLICT(tilename) DO UPDATE
                                                    SET geotiff_link = EXCLUDED.geotiff_link,
                                                    rat_link = EXCLUDED.rat_link,
                                                    delivered_date = EXCLUDED.delivered_date,
                                                    resolution = EXCLUDED.resolution,
                                                    utm = EXCLUDED.utm,
                                                    subregion = EXCLUDED.subregion,
                                                    geotiff_disk = Null,
                                                    rat_disk = Null''', insert_tiles)
        registry_connection.commit()
    gdal.Unlink(global_tileset)

def to_base(charset, n, fill):
    res = ""
    while n:
        res+=charset[n%len(charset)]
        n//=len(charset)
    return (res[::-1] or charset[0]).rjust(fill, charset[0])

def global_region_tileset(index, size) -> str:
    """
    Parameters
    ----------
    index
        index of tileset
    size
        length of individual tile
    Returns
    -------
    gdal memory filepath to produced global tileset
    """
    charset="BCDFGHJKLMNPQRSTVWXZ"
    name = to_base(charset, index, 2)
    roundnum = len(size.split('.')[1])
    size = float(size)
    location = "/vsimem/global_tileset.gpkg"
    ds = ogr.GetDriverByName('GPKG').CreateDataSource(location)
    srs = osr.SpatialReference()
    srs.ImportFromEPSG(4326)
    layer = ds.CreateLayer('global_tileset', srs, ogr.wkbMultiPolygon)
    layer.CreateFields([ogr.FieldDefn("Region", ogr.OFTString),
                        ogr.FieldDefn("UTM_Zone", ogr.OFTInteger), 
                        ogr.FieldDefn("Hemisphere", ogr.OFTString)])
    layer_defn = layer.GetLayerDefn()
    layer.StartTransaction()
    y = round(-90+size, roundnum)
    y_count = 0
    while y <= 90:
        ns = "N"
        if y <= 0:
            ns = "S"
        x = -180
        x_count = 0
        while x < 180:
            current_utm = "{:02d}".format(int(np.ceil((180+x+.00000001)/6)))
            ring = ogr.Geometry(ogr.wkbLinearRing)
            ring.AddPoint_2D(x, y)
            ring.AddPoint_2D(round(x+size,roundnum), y)
            ring.AddPoint_2D(round(x+size,roundnum), round(y-size,roundnum))
            ring.AddPoint_2D(x, round(y-size,roundnum))
            ring.AddPoint_2D(x, y)
            poly = ogr.Geometry(ogr.wkbPolygon)
            poly.AddGeometry(ring)
            poly = poly.Buffer(-.002)
            multipoly = ogr.Geometry(ogr.wkbMultiPolygon)
            multipoly.AddGeometry(poly)
            feat = ogr.Feature(layer_defn)
            feat.SetGeometry(multipoly)
            charset="2456789BCDFGHJKLMNPQRSTVWXZ"
            x_rep = to_base(charset, x_count, 3)
            y_rep = to_base(charset, y_count, 3)
            feat.SetField('Region', f'{name}{x_rep}{y_rep}')
            feat.SetField('UTM_Zone', current_utm)
            feat.SetField('Hemisphere', ns)
            layer.CreateFeature(feat)
            x = round(x+size, roundnum)
            x_count += 1
        y = round(y+size, roundnum)
        y_count += 1
    layer.CommitTransaction()
    return location

def sweep_files(registry_connection: sqlite3.Connection, bluetopo_path: str) -> None:
    """
    Parameters
    ----------
    registry_connection
        A database connection object
    bluetopo_path
        destination directory for project's output
    Returns
    -------
    None
    """
    db_tiles = all_db_tiles(registry_connection)
    cursor = registry_connection.cursor()
    cursor.row_factory = sqlite3.Row
    removed_tiles = 0
    removed_subregions = 0
    removed_utms = 0
    for tile_fields in db_tiles:
        if ((tile_fields['geotiff_disk'] 
            and os.path.isfile(os.path.join(bluetopo_path, tile_fields['geotiff_disk'])) == False) or 
           (tile_fields['rat_disk']
            and os.path.isfile(os.path.join(bluetopo_path, tile_fields['rat_disk'])) == False)):
            cursor.execute('DELETE FROM tiles where tilename = ? RETURNING *', (tile_fields['tilename'],))
            deleted_tile = cursor.fetchone()
            if deleted_tile:
                removed_tiles += 1
                files = ['geotiff_disk', 'rat_disk']
                for file in files:
                    try:
                        if (deleted_tile[file] 
                        and os.path.isfile(os.path.join(bluetopo_path, deleted_tile[file]))):
                            os.remove(os.path.join(bluetopo_path, deleted_tile[file]))
                    except (OSError, PermissionError):
                        continue
            cursor.execute('''DELETE FROM vrt_subregion 
                            WHERE region NOT IN 
                            (SELECT subregion FROM tiles where geotiff_disk is not null and rat_disk is not null) 
                            RETURNING *;''')
            deleted_subregions = cursor.fetchall()
            removed_subregions += len(deleted_subregions)
            for deleted_subregion in deleted_subregions:
                files = ['res_2_vrt', 'res_2_ovr', 'res_4_vrt', 'res_4_ovr', 
                        'res_8_vrt', 'res_8_ovr', 'complete_vrt', 'complete_ovr']
                for file in files:
                    try:
                        if (deleted_subregion[file] 
                        and os.path.isfile(os.path.join(bluetopo_path, deleted_subregion[file]))):
                            os.remove(os.path.join(bluetopo_path, deleted_subregion[file]))
                    except (OSError, PermissionError):
                        continue
            cursor.execute('''DELETE FROM vrt_utm 
                            WHERE utm NOT IN 
                            (SELECT utm FROM tiles where geotiff_disk is not null and rat_disk is not null) 
                            RETURNING *;''')
            deleted_utms = cursor.fetchall()
            removed_utms += len(deleted_utms)
            for deleted_utm in deleted_utms:
                files = ['utm_vrt', 'utm_ovr']
                for file in files:
                    try:
                        if deleted_utm[file] and os.path.isfile(os.path.join(bluetopo_path, deleted_utm[file])):
                            os.remove(os.path.join(bluetopo_path, deleted_utm[file]))
                    except (OSError, PermissionError):
                        continue
            registry_connection.commit()
    return removed_tiles, removed_subregions, removed_utms

def main(destination_path: str, desired_area_filename: str = None, remove_missing_files: bool = False) -> [[str],[str]]:
    """
    Parameters
    ----------
    destination_path : str
        destination path for the downloaded files.
    desired_area_filename : str
        a gdal compatible file path denoting geometries that reflect the region of interest.
    remove_missing_files: bool
        if true, tiles that are missing files will be removed from sqlite database rather than redownloaded and 
        vrts will no longer be created with that tile when build_vrt is run.
    Returns
    -------
    [[list of tiles found],[list of tiles not found]]
    """
    start = datetime.datetime.now()
    print(f'Beginning work on {destination_path}')
    if not os.path.exists(destination_path):
        os.makedirs(destination_path)
    conn = connect_to_survey_registry(destination_path)
    bluetopo_geometry_filename = download_bluetopo_tesselation(conn, destination_path)
    if remove_missing_files:
        removed_tiles, removed_subregions, removed_utms = sweep_files(conn, destination_path)
        print(f'removed {removed_tiles} tile(s), {removed_subregions} subregion vrt(s), {removed_utms} utm vrt(s)')
    if desired_area_filename:
        if not os.path.isfile(desired_area_filename):
            raise ValueError(f'''The geometry file {desired_area_filename} for determining what to download does not exist.''')
        tile_list = get_tile_list(desired_area_filename, bluetopo_geometry_filename)
        delivered_tile_count = insert_new(conn, tile_list)
        print(f'''{delivered_tile_count} available BlueTopo tile(s) discovered in a total of {len(tile_list)} intersected tile(s) with given polygon.''')
    upsert_tiles(bluetopo_geometry_filename, destination_path, conn)
    existing_tiles, tiles_found, tiles_not_found = download_bluetopo_tiles(conn, destination_path)
    done = datetime.datetime.now()
    lapse = done - start
    print(f'\nOperation complete after {lapse} with {len(existing_tiles)} already existing tiles, {len(tiles_found)} tiles downloaded and {len(tiles_not_found)} '
          f'not available on AWS.')
    return tiles_found, tiles_not_found