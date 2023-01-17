"""
fetch_tiles.py

0.0.1 20220614

glen.rice@noaa.gov 20220614

An example script for downloading BlueTopo datasets from AWS.

"""

import boto3
import datetime
import numpy as np
import os
import sqlite3
import sys
from botocore import UNSIGNED
from botocore.client import Config
from nbs.bluetopo.build_vrt import connect_to_survey_registry
from osgeo import ogr, osr, gdal

debug_info = f"""
        Python {sys.version_info.major}.{sys.version_info.minor}.{sys.version_info.micro} 
        GDAL {gdal.VersionInfo()} 
        SQLite {sqlite3.sqlite_version}
        Date {datetime.datetime.now()}
        """

def get_tesselation(conn: sqlite3.Connection, root: str, 
                    bucket: str = 'noaa-ocs-nationalbathymetry-pds',
                    prefix: str = 'BlueTopo/_BlueTopo_Tile_Scheme/BlueTopo_Tile_Scheme',
                    ) -> str:
    """ 
    Download the BlueTopo tesselation scheme geopackage from AWS.

    Parameters
    ----------
    conn : sqlite3.Connection
        A database connection object.
    root : str
        destination directory for project.
    bucket : str
        AWS bucket for the National Bathymetric Source project.
    prefix : str
        The prefix for the geopackage on AWS to find the file.

    Returns
    -------
    destination_name : str
        the downloaded file path string.
    """
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM tileset")
    for tilescheme in [dict(row) for row in cursor.fetchall()]:
        try:
            os.remove(os.path.join(root, tilescheme["location"]))
        except (OSError, PermissionError):
            continue
    cred = {"aws_access_key_id": "", 
            "aws_secret_access_key": "", 
            "config": Config(signature_version=UNSIGNED)}
    client = boto3.client("s3", **cred)
    pageinator = client.get_paginator("list_objects_v2")
    objs = pageinator.paginate(Bucket=bucket, Prefix=prefix).build_full_result()
    if len(objs) == 0:
        print(f"No geometry found in {prefix}")
        return None
    source_name = objs["Contents"][0]["Key"]
    path, filename = os.path.split(source_name)
    if len(objs) > 1:
        print(f"More than one geometry found in {prefix}, using {filename}")
    destination_name = os.path.join(root, source_name)
    if not os.path.exists(os.path.dirname(destination_name)):
        os.makedirs(os.path.dirname(destination_name))
    try:
        client.download_file(bucket, source_name, destination_name)
    except (OSError, PermissionError) as e:
        print(f"Failed to download BlueTopo tile scheme "
               "possibly due to conflict with an open existing file. "
               "Please close all files and attempt again")
        sys.exit(1)
    print(f"downloaded {filename}")
    cursor.execute("""REPLACE INTO tileset(tilescheme, location, downloaded) 
                      VALUES(?, ?, ?)""",
                    ("BlueTopo", source_name, datetime.datetime.now()))
    conn.commit()
    return destination_name


def download_tiles(conn: sqlite3.Connection, root: str,
                   bucket: str = 'noaa-ocs-nationalbathymetry-pds') -> [[str],[str],[str]]:
    """ 
    Download BlueTopo tiles' files (geotiff and aux per tile).

    Parameters
    ----------
    conn : sqlite3.Connection
        A database connection object.
    root : str
        destination directory for project.
    bucket : str
        AWS bucket for the National Bathymetric Source project.

    Returns
    -------
    existing_tiles : list
        tiles already existing locally.
    tiles_found : list
        tiles found in s3 bucket.
    tiles_not_found : list
        tiles not found in s3 bucket.
    """
    download_tile_list = all_db_tiles(conn)
    new_tile_list = [download_tile for download_tile in download_tile_list 
                     if download_tile["geotiff_disk"] is None 
                     or download_tile["rat_disk"] is None]
    print(f"{len(new_tile_list)} new tiles being downloaded")
    cred = {"aws_access_key_id": "", 
            "aws_secret_access_key": "", 
            "config": Config(signature_version=UNSIGNED)}
    client = boto3.client("s3", **cred)
    pageinator = client.get_paginator("list_objects_v2")
    existing_tiles = []
    tiles_found = []
    tiles_not_found = []
    display_count = 1
    msg = ""
    for tile_fields in download_tile_list:
        if (tile_fields["geotiff_disk"] and tile_fields["rat_disk"] 
        and os.path.isfile(os.path.join(root, tile_fields["geotiff_disk"])) 
        and os.path.isfile(os.path.join(root, tile_fields["rat_disk"]))):
            existing_tiles.append(tile_fields["tilename"])
            continue
        tilename = tile_fields["tilename"]
        prefix = f"BlueTopo/{tilename}/"
        objs = pageinator.paginate(Bucket=bucket, Prefix=prefix).build_full_result()
        if len(objs) > 0:
            for object_name in objs["Contents"]:
                source_name = object_name["Key"]
                dest = os.path.join("BlueTopo", 
                                   f"UTM{tile_fields['utm']}", 
                                    os.path.basename(source_name))
                destination_name = os.path.join(root, dest)
                if not os.path.exists(os.path.dirname(destination_name)):
                    os.makedirs(os.path.dirname(destination_name))
                remove_previous = len(msg) * '\b'
                msg = f"downloading{display_count * '.'}"
                print(remove_previous + msg, end="")
                display_count += 1
                client.download_file(bucket, source_name, destination_name)
                if ".aux" in destination_name.lower():
                    tile_fields["rat_disk"] = dest
                else:
                    tile_fields["geotiff_disk"] = dest
            update_records(conn, tile_fields)
            tiles_found.append(tilename)
        else:
            tiles_not_found.append(tilename)
        if display_count > 10:
            display_count = 1
    return existing_tiles, tiles_found, tiles_not_found


def get_tile_list(desired_area_filename: str, tile_scheme_filename: str) -> [str]:
    """ 
    Get the list of tiles inside the given polygon(s).

    Parameters
    ----------
    desired_area_filename : str
        a gdal compatible file path denoting geometries that reflect the region of interest.
    tile_scheme_filename : str
        a gdal compatible file path denoting geometries that reflect the tesselation scheme 
        with addressing information for the desired tiles.

    Returns
    -------
    feature_list : str
        list of tiles intersecting with the provided polygon(s).
    """
    target = ogr.Open(desired_area_filename)
    if target is None:
        print("Unable to open desired area file")
        return None
    source = ogr.Open(tile_scheme_filename)
    if source is None:
        print("Unable to open tile scheme file")
        return None
    driver = ogr.GetDriverByName("MEMORY")
    intersection = driver.CreateDataSource("memData")
    intersect_lyr = intersection.CreateLayer("intersect_lyr", geom_type=ogr.wkbPolygon)
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
        lyr_defn = intersect_lyr.GetLayerDefn()
        for feature in intersect_lyr:
            field_list = {}
            for field_num in range(lyr_defn.GetFieldCount()):
                field_list[lyr_defn.GetFieldDefn(field_num).name] = feature.GetField(field_num)
            feature_list.append(field_list)
    return feature_list

def transform_layer(input_layer: ogr.Layer, desired_crs: osr.SpatialReference) -> ogr.DataSource:
    """ 
    Transform a provided ogr layer to the provide osr coordinate reference system.

    Parameters
    ----------
    input_layer : ogr.Layer
        the ogr layer to be transformed.
    desired_crs : osr.SpatialReference
        the target coordinate system for the transform.

    Returns
    -------
    output_ds : ogr.DataSource
        transformed ogr memory datasource.
    """
    target_crs = input_layer.GetSpatialRef()
    coord_trans = osr.CoordinateTransformation(target_crs, desired_crs)
    driver = ogr.GetDriverByName("MEMORY")
    output_ds = driver.CreateDataSource("memData")
    output_lyr = output_ds.CreateLayer("output_lyr", geom_type=input_layer.GetGeomType())
    outlayer_defn = output_lyr.GetLayerDefn()
    in_feature = input_layer.GetNextFeature()
    while in_feature:
        geom = in_feature.GetGeometryRef()
        geom.Transform(coord_trans)
        out_feature = ogr.Feature(outlayer_defn)
        out_feature.SetGeometry(geom)
        output_lyr.CreateFeature(out_feature)
        out_feature = None
        in_feature = input_layer.GetNextFeature()
    return output_ds

def update_records(conn: sqlite3.Connection, tile: dict) -> None:
    """ 
    Update BlueTopo tile record and associated tables in SQLite database.

    Parameters
    ----------
    conn : sqlite3.Connection
        A database connection object.
    tile : dict
        BlueTopo tile record.
    """
    cursor = conn.cursor()
    cursor.execute("UPDATE tiles SET geotiff_disk = ?, rat_disk = ? where tilename = ?", 
                  (tile["geotiff_disk"], tile["rat_disk"],tile["tilename"],))
    cursor.execute("""INSERT INTO vrt_subregion(region, utm, res_2_vrt, res_2_ovr, 
                      res_4_vrt, res_4_ovr, res_8_vrt, res_8_ovr, 
                      complete_vrt, complete_ovr, built)
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
                      built = EXCLUDED.built""",
                     (tile["subregion"], tile["utm"], 
                      None, None, None, None, None, None, None, None, 0))
    cursor.execute("""INSERT INTO vrt_utm(utm, utm_vrt, utm_ovr, built)
                      VALUES(?, ?, ?, ?)
                      ON CONFLICT(utm) DO UPDATE
                      SET utm_vrt = EXCLUDED.utm_vrt,
                      utm_ovr = EXCLUDED.utm_ovr,
                      built = EXCLUDED.built""",
                     (tile["utm"], None, None, 0))
    conn.commit()

def insert_new(conn: sqlite3.Connection, tiles: list) -> int:
    """ 
    Insert new BlueTopo tile records into SQLite database.

    Parameters
    ----------
    conn : sqlite3.Connection
        A database connection object.
    tiles : list of dict
        List of BlueTopo tile records.

    Returns
    -------
    tile_list : int
        amount of delivered tiles from input tiles.
    """
    cursor = conn.cursor()
    tile_list = [(tile["tile"],) for tile in tiles if tile["Delivered_Date"] 
                        and tile["GeoTIFF_Link"] and tile["RAT_Link"]]
    cursor.executemany("""INSERT INTO tiles(tilename) 
                          VALUES(?) ON CONFLICT DO NOTHING""", tile_list)
    conn.commit()
    return len(tile_list)

def all_db_tiles(conn: sqlite3.Connection) -> list:
    """ 
    Retrieve all tile records in tiles table of SQLite database.

    Parameters
    ----------
    conn : sqlite3.Connection
        A database connection object.

    Returns
    -------
    list
        all tile records as dictionaries.
    """
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM tiles")
    return [dict(row) for row in cursor.fetchall()]

def upsert_tiles(tile_scheme: str, root: str, conn: sqlite3.Connection) -> None:
    """ 
    Update tile records in SQLite database with latest deliveries found in tilescheme.

    Parameters
    ----------
    tile_scheme : str
        a gdal compatible file path with the BlueTopo tesselation scheme
    root : str
        destination directory for project.
    conn : sqlite3.Connection
        A database connection object.
    """
    cursor = conn.cursor()
    db_tiles = all_db_tiles(conn)
    bluetopo_ds = ogr.Open(tile_scheme)
    bluetopo_lyr = bluetopo_ds.GetLayer()
    lyr_def = bluetopo_lyr.GetLayerDefn()
    bt_tiles = []
    for ft in bluetopo_lyr:
        field_list = {}
        geom = ft.GetGeometryRef()
        field_list["wkt_geom"] = geom.ExportToWkt()
        for field_num in range(lyr_def.GetFieldCount()):
            field_name = lyr_def.GetFieldDefn(field_num).name
            field_list[field_name.lower()] = ft.GetField(field_name)
        bt_tiles.append(field_list)
    bluetopo_ds = None
    global_tileset = global_region_tileset(1, "1.2")
    gs = ogr.Open(global_tileset)
    lyr = gs.GetLayer()
    insert_tiles = []
    for db_tile in db_tiles:
        bt_tile = [bt_tile for bt_tile in bt_tiles if db_tile["tilename"] == bt_tile["tile"]]
        if len(bt_tile) == 0:
            print(f"Warning: {db_tile['tilename']} in database appears to have "
                   "been removed from latest BlueTopo tilescheme")
            continue
        if len(bt_tile) > 1:
            raise ValueError(f"""More than one tilename {db_tile['tilename']} found in tileset. 
                                 Please alert NBS. 
                                 {debug_info}""")
        # inserted only when delivered exists 
        # so indicates delivered date removed post-insertion to None.
        if bt_tile[0]["delivered_date"] is None:
            print(f"Warning: Unexpected removal of delivered date for tile {db_tile['tilename']}")
            continue
        if ((db_tile["delivered_date"] is None) or 
        (bt_tile[0]["delivered_date"] > db_tile["delivered_date"])):
            try:
                if (db_tile["geotiff_disk"] and 
                os.path.isfile(os.path.join(root, db_tile["geotiff_disk"]))):
                    os.remove(os.path.join(root, db_tile["geotiff_disk"]))
                if (db_tile["rat_disk"] and 
                os.path.isfile(os.path.join(root, db_tile["rat_disk"]))):
                    os.remove(os.path.join(root, db_tile["rat_disk"]))
            except (OSError, PermissionError) as e:
                print(f"failed to remove older files for tile {db_tile['tilename']}. " 
                       "please close all files and attempt fetch again.")
                gdal.Unlink(global_tileset)
                raise e
            lyr.SetSpatialFilter(ogr.CreateGeometryFromWkt(bt_tile[0]["wkt_geom"]))
            if lyr.GetFeatureCount() != 1:
                gdal.Unlink(global_tileset)
                raise ValueError(f"Error getting subregion for {db_tile['tilename']}. "
                                 f"{lyr.GetFeatureCount()} subregion(s). "
                                 f"{debug_info}")
            region_ft = lyr.GetNextFeature()
            bt_tile[0]["region"] = region_ft.GetField("Region")
            insert_tiles.append((bt_tile[0]["tile"], bt_tile[0]["geotiff_link"],
                                 bt_tile[0]["rat_link"], bt_tile[0]["delivered_date"], 
                                 bt_tile[0]["resolution"], bt_tile[0]["utm"], 
                                 bt_tile[0]["region"],))
    if insert_tiles:
        cursor.executemany("""INSERT INTO tiles(tilename, geotiff_link, rat_link, 
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
                              rat_disk = Null""",
                              insert_tiles)
        conn.commit()
    gdal.Unlink(global_tileset)

def convert_base(charset: str, input: int, fill: int) -> str:
    """ 
    Convert integer to new base system using the given symbols with a minimum filled length.

    Parameters
    ----------
    charset : str
        length of this str will be the new base system and characters 
        given will be the symbols used.
    input : int
        integer to convert.
    fill : int
        returned output will be adjusted to this desired length with 
        fill values of the lowest value in charset.

    Returns
    -------
    str
        converted value in given system.
    """
    res = ""
    while input:
        res+=charset[input%len(charset)]
        input//=len(charset)
    return (res[::-1] or charset[0]).rjust(fill, charset[0])

def global_region_tileset(index: int, size: str) -> str:
    """ 
    Generate a global tilescheme.

    Parameters
    ----------
    index : int
        index of tileset to determine tilescheme name.
    size : str
        length of the side of an individual tile in degrees.

    Returns
    -------
    location : str
        gdal memory filepath to global tilescheme.
    """
    charset="BCDFGHJKLMNPQRSTVWXZ"
    name = convert_base(charset, index, 2)
    roundnum = len(size.split(".")[1])
    size = float(size)
    location = "/vsimem/global_tileset.gpkg"
    ds = ogr.GetDriverByName("GPKG").CreateDataSource(location)
    srs = osr.SpatialReference()
    srs.ImportFromEPSG(4326)
    layer = ds.CreateLayer("global_tileset", srs, ogr.wkbMultiPolygon)
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
            x_rep = convert_base(charset, x_count, 3)
            y_rep = convert_base(charset, y_count, 3)
            feat.SetField("Region", f"{name}{x_rep}{y_rep}")
            feat.SetField("UTM_Zone", current_utm)
            feat.SetField("Hemisphere", ns)
            layer.CreateFeature(feat)
            x = round(x+size, roundnum)
            x_count += 1
        y = round(y+size, roundnum)
        y_count += 1
    layer.CommitTransaction()
    return location

def sweep_files(conn: sqlite3.Connection, root: str) -> None:
    """ 
    Remove missing files from tracking.

    Parameters
    ----------
    conn : sqlite3.Connection
        A database connection object.
    root : str
        destination directory for project.
    """
    db_tiles = all_db_tiles(conn)
    cursor = conn.cursor()
    removed_tiles = 0
    removed_subregions = 0
    removed_utms = 0
    for tile_fields in db_tiles:
        if ((tile_fields["geotiff_disk"] 
            and os.path.isfile(os.path.join(root, tile_fields["geotiff_disk"])) == False) or 
           (tile_fields["rat_disk"]
            and os.path.isfile(os.path.join(root, tile_fields["rat_disk"])) == False)):
            cursor.execute("DELETE FROM tiles where tilename = ? RETURNING *", 
                           (tile_fields["tilename"],))
            deleted_tile = cursor.fetchone()
            if deleted_tile:
                removed_tiles += 1
                files = ["geotiff_disk", "rat_disk"]
                for file in files:
                    try:
                        if (deleted_tile[file] 
                        and os.path.isfile(os.path.join(root, deleted_tile[file]))):
                            os.remove(os.path.join(root, deleted_tile[file]))
                    except (OSError, PermissionError):
                        continue
            cursor.execute("""DELETE FROM vrt_subregion 
                            WHERE region NOT IN 
                            (SELECT subregion 
                             FROM tiles 
                             WHERE geotiff_disk is not null AND rat_disk is not null)
                            RETURNING *;""")
            deleted_subregions = cursor.fetchall()
            removed_subregions += len(deleted_subregions)
            for deleted_subregion in deleted_subregions:
                files = ["res_2_vrt", "res_2_ovr", "res_4_vrt", "res_4_ovr", 
                        "res_8_vrt", "res_8_ovr", "complete_vrt", "complete_ovr"]
                for file in files:
                    try:
                        if (deleted_subregion[file] 
                        and os.path.isfile(os.path.join(root, deleted_subregion[file]))):
                            os.remove(os.path.join(root, deleted_subregion[file]))
                    except (OSError, PermissionError):
                        continue
            cursor.execute("""DELETE FROM vrt_utm 
                            WHERE utm NOT IN 
                            (SELECT utm 
                             FROM tiles 
                             WHERE geotiff_disk is not null AND rat_disk is not null) 
                            RETURNING *;""")
            deleted_utms = cursor.fetchall()
            removed_utms += len(deleted_utms)
            for deleted_utm in deleted_utms:
                files = ["utm_vrt", "utm_ovr"]
                for file in files:
                    try:
                        if ((deleted_utm[file]) and 
                        (os.path.isfile(os.path.join(root, deleted_utm[file])))):
                            os.remove(os.path.join(root, deleted_utm[file]))
                    except (OSError, PermissionError):
                        continue
            conn.commit()
    return removed_tiles, removed_subregions, removed_utms

def main(destination_path: str, 
         desired_area_filename: str = None, 
         remove_missing_files: bool = False) -> [[str],[str]]:
    """ 
    Track tiles. Download tiles. Update already tracked files.

    Parameters
    ----------
    destination_path : str
        destination directory for project.
    desired_area_filename : str
        a gdal compatible file path denoting geometries that reflect the region of interest.
    remove_missing_files : bool
        if true, files that are missing or deleted will be removed from tracking rather than 
        remaining in system to be potentially redownloaded.

    Returns
    -------
    tiles_found : list
        tiles downloaded.
    tiles_not_found : list
        tiles not downloaded.
    """
    start = datetime.datetime.now()
    print(f"Beginning work on {destination_path}")
    if not os.path.exists(destination_path):
        os.makedirs(destination_path)
    conn = connect_to_survey_registry(destination_path)
    bluetopo_geometry_filename = get_tesselation(conn, destination_path)
    if remove_missing_files:
        removed_tiles, removed_subregions, removed_utms = sweep_files(conn, destination_path)
        print(f"Removed {removed_tiles} tile(s), "
              f"{removed_subregions} subregion vrt(s), "
              f"{removed_utms} utm vrt(s)")
    if desired_area_filename:
        if not os.path.isfile(desired_area_filename):
            raise ValueError(f"The geometry {desired_area_filename} for "
                              "determining what to download does not exist.")
        tile_list = get_tile_list(desired_area_filename, bluetopo_geometry_filename)
        delivered_tile_count = insert_new(conn, tile_list)
        print(f"{delivered_tile_count} available BlueTopo tile(s) discovered " 
              f"in a total of {len(tile_list)} intersected tile(s) with given polygon.")
    upsert_tiles(bluetopo_geometry_filename, destination_path, conn)
    existing_tiles, tiles_found, tiles_not_found = download_tiles(conn, destination_path)
    done = datetime.datetime.now()
    lapse = done - start
    print(f"\nOperation complete after {lapse} with {len(existing_tiles)} already existing tiles, "
          f"{len(tiles_found)} tiles downloaded and {len(tiles_not_found)} "
          f"not available on AWS.")
    return tiles_found, tiles_not_found