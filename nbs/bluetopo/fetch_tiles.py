"""
fetch_tiles.py

0.0.1 20220614

glen.rice@noaa.gov 20220614

An example script for downloading BlueTopo (and Modeling) datasets from AWS.

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


def get_tessellation(conn: sqlite3.Connection,
                     root: str,
                     prefix: str,
                     target: str,
                     bucket: str = 'noaa-ocs-nationalbathymetry-pds'
                    ) -> str:
    """
    Download the tessellation scheme geopackage from AWS.

    Parameters
    ----------
    conn : sqlite3.Connection
        database connection object.
    root : str
        destination directory for project.
    prefix : str
        the prefix for the geopackage on AWS to find the file.
    target : str
        the datasource the script will target e.g. 'BlueTopo' or 'Modeling'.
    bucket : str
        AWS bucket for the National Bathymetric Source project.

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
    filename = os.path.basename(source_name)
    relative = os.path.join(target, f"Tessellation", filename)
    if len(objs) > 1:
        print(f"More than one geometry found in {prefix}, using {filename}")
    destination_name = os.path.join(root, relative)
    if not os.path.exists(os.path.dirname(destination_name)):
        os.makedirs(os.path.dirname(destination_name))
    try:
        client.download_file(bucket, source_name, destination_name)
    except (OSError, PermissionError) as e:
        print(f"Failed to download tile scheme "
               "possibly due to conflict with an open existing file. "
               "Please close all files and attempt again")
        sys.exit(1)
    print(f"downloaded {filename}")
    cursor.execute("""REPLACE INTO tileset(tilescheme, location, downloaded)
                      VALUES(?, ?, ?)""",
                    ("Tessellation", relative, datetime.datetime.now()))
    conn.commit()
    return destination_name


def download_tiles(conn: sqlite3.Connection,
                   root: str,
                   tile_prefix: str,
                   target: str,
                   bucket: str = 'noaa-ocs-nationalbathymetry-pds'
                  ) -> [[str],[str],[str]]:
    """
    Download tiles' files (geotiff and aux per tile).

    Parameters
    ----------
    conn : sqlite3.Connection
        database connection object.
    root : str
        destination directory for project.
    tile_prefix : str
        s3 prefix for tiles.
    target : str
        the datasource the script will target e.g. 'BlueTopo' or 'Modeling'.
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
    for fields in download_tile_list:
        if (fields["geotiff_disk"] and fields["rat_disk"]
        and os.path.isfile(os.path.join(root, fields["geotiff_disk"]))
        and os.path.isfile(os.path.join(root, fields["rat_disk"]))):
            existing_tiles.append(fields["tilename"])
            continue
        tilename = fields["tilename"]
        pfx = tile_prefix + f"/{tilename}/"
        objs = pageinator.paginate(Bucket=bucket, Prefix=pfx).build_full_result()
        if len(objs) > 0:
            for object_name in objs["Contents"]:
                source_name = object_name["Key"]
                relative = os.path.join(target,
                                   f"UTM{fields['utm']}",
                                    os.path.basename(source_name))
                destination_name = os.path.join(root, relative)
                if not os.path.exists(os.path.dirname(destination_name)):
                    os.makedirs(os.path.dirname(destination_name))
                remove_previous = len(msg) * '\b'
                msg = f"downloading new and any missing tracked tiles{display_count * '.'}"
                print(remove_previous + msg, end="")
                display_count += 1
                client.download_file(bucket, source_name, destination_name)
                if ".aux" in destination_name.lower():
                    fields["rat_disk"] = relative
                else:
                    fields["geotiff_disk"] = relative
            update_records(conn, fields)
            tiles_found.append(tilename)
        else:
            tiles_not_found.append(tilename)
        if display_count > 10:
            display_count = 1
    return existing_tiles, tiles_found, tiles_not_found


def get_tile_list(desired_area_filename: str,
                  tile_scheme_filename: str
                 ) -> [str]:
    """
    Get the list of tiles inside the given polygon(s).

    Parameters
    ----------
    desired_area_filename : str
        a gdal compatible file path denoting geometries that reflect the region
        of interest.
    tile_scheme_filename : str
        a gdal compatible file path denoting geometries that reflect the
        tessellation scheme with addressing information for the desired tiles.

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
    intersect = driver.CreateDataSource("memData")
    intersect_lyr = intersect.CreateLayer("mem", geom_type=ogr.wkbPolygon)
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
            fields = {}
            for idx in range(lyr_defn.GetFieldCount()):
                fields[lyr_defn.GetFieldDefn(idx).name] = feature.GetField(idx)
            feature_list.append(fields)
    return feature_list


def transform_layer(input_layer: ogr.Layer,
                    desired_crs: osr.SpatialReference
                   ) -> ogr.DataSource:
    """
    Transform a provided ogr layer to the provided coordinate reference system.

    Parameters
    ----------
    input_layer : ogr.Layer
        the ogr layer to be transformed.
    desired_crs : osr.SpatialReference
        the target coordinate system for the transform.

    Returns
    -------
    out_ds : ogr.DataSource
        transformed ogr memory datasource.
    """
    target_crs = input_layer.GetSpatialRef()
    coord_trans = osr.CoordinateTransformation(target_crs, desired_crs)
    driver = ogr.GetDriverByName("MEMORY")
    out_ds = driver.CreateDataSource("memData")
    out_lyr = out_ds.CreateLayer("out_lyr", geom_type=input_layer.GetGeomType())
    out_defn = out_lyr.GetLayerDefn()
    in_feature = input_layer.GetNextFeature()
    while in_feature:
        geom = in_feature.GetGeometryRef()
        geom.Transform(coord_trans)
        out_feature = ogr.Feature(out_defn)
        out_feature.SetGeometry(geom)
        out_lyr.CreateFeature(out_feature)
        out_feature = None
        in_feature = input_layer.GetNextFeature()
    return out_ds


def update_records(conn: sqlite3.Connection, tile: dict) -> None:
    """
    Update tile record and associated tables in SQLite database.

    Parameters
    ----------
    conn : sqlite3.Connection
        database connection object.
    tile : dict
        tile record.
    """
    cursor = conn.cursor()
    cursor.execute("""UPDATE tiles
                      SET geotiff_disk = ?, rat_disk = ?
                      WHERE tilename = ?""",
                  (tile["geotiff_disk"], tile["rat_disk"],tile["tilename"],))
    cursor.execute("""INSERT INTO vrt_subregion(region, utm, res_2_vrt,
                      res_2_ovr, res_4_vrt, res_4_ovr, res_8_vrt, res_8_ovr,
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
    Insert new tile records into SQLite database.

    Parameters
    ----------
    conn : sqlite3.Connection
        database connection object.
    tiles : list of dict
        list of tile records.

    Returns
    -------
    int
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
        database connection object.

    Returns
    -------
    list
        all tile records as dictionaries.
    """
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM tiles")
    return [dict(row) for row in cursor.fetchall()]


def upsert_tiles(conn: sqlite3.Connection, root: str, tile_scheme: str) -> None:
    """
    Update tile records in database with latest deliveries found in tilescheme.

    Parameters
    ----------
    conn : sqlite3.Connection
        database connection object.
    root : str
        destination directory for project.
    tile_scheme : str
        a gdal compatible file path with the tessellation scheme.
    """
    # database records holds current set
    # tilescheme polygons has latest set
    # use the two to see where new tiles or updates to existing tiles exist
    # use global tileset to map its region
    db_tiles = all_db_tiles(conn)
    ts_ds = ogr.Open(tile_scheme)
    ts_lyr = ts_ds.GetLayer()
    ts_defn = ts_lyr.GetLayerDefn()
    ts_tiles = []
    for ft in ts_lyr:
        field_list = {}
        geom = ft.GetGeometryRef()
        field_list["wkt_geom"] = geom.ExportToWkt()
        for field_num in range(ts_defn.GetFieldCount()):
            field_name = ts_defn.GetFieldDefn(field_num).name
            field_list[field_name.lower()] = ft.GetField(field_name)
        ts_tiles.append(field_list)
    ts_ds = None
    global_tileset = global_region_tileset(1, "1.2")
    gs = ogr.Open(global_tileset)
    lyr = gs.GetLayer()
    insert_tiles = []
    for db_tile in db_tiles:
        ts_tile = [ts_tile for ts_tile in ts_tiles
                           if db_tile["tilename"] == ts_tile["tile"]]
        if len(ts_tile) == 0:
            print(f"Warning: {db_tile['tilename']} in database appears to have "
                   "been removed from latest tilescheme")
            continue
        if len(ts_tile) > 1:
            raise ValueError(f"More than one tilename {db_tile['tilename']} "
                              "found in tileset.\n"
                              "Please alert NBS.\n"
                              "{debug_info}")
        ts_tile = ts_tile[0]
        # inserted into db only when delivered_date exists
        # so value of None in ts_tile indicates delivered_date was removed
        if ts_tile["delivered_date"] is None:
            print("Warning: Unexpected removal of delivered date "
                 f"for tile {db_tile['tilename']}")
            continue
        if ((db_tile["delivered_date"] is None) or
        (ts_tile["delivered_date"] > db_tile["delivered_date"])):
            try:
                if (db_tile["geotiff_disk"] and
                os.path.isfile(os.path.join(root, db_tile["geotiff_disk"]))):
                    os.remove(os.path.join(root, db_tile["geotiff_disk"]))
                if (db_tile["rat_disk"] and
                os.path.isfile(os.path.join(root, db_tile["rat_disk"]))):
                    os.remove(os.path.join(root, db_tile["rat_disk"]))
            except (OSError, PermissionError) as e:
                print("Failed to remove older files for tile "
                     f"{db_tile['tilename']}. Please close all files and "
                      "attempt fetch again.")
                gdal.Unlink(global_tileset)
                raise e
            lyr.SetSpatialFilter(ogr.CreateGeometryFromWkt(ts_tile["wkt_geom"]))
            if lyr.GetFeatureCount() != 1:
                gdal.Unlink(global_tileset)
                raise ValueError("Error getting subregion for "
                                f"{db_tile['tilename']}. \n"
                                f"{lyr.GetFeatureCount()} subregion(s). \n"
                                f"{debug_info}")
            region_ft = lyr.GetNextFeature()
            ts_tile["region"] = region_ft.GetField("Region")
            insert_tiles.append((ts_tile["tile"], ts_tile["geotiff_link"],
                                 ts_tile["rat_link"], ts_tile["delivered_date"],
                                 ts_tile["resolution"], ts_tile["utm"],
                                 ts_tile["region"],))
    if insert_tiles:
        cursor = conn.cursor()
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


def convert_base(charset: str, input: int, minimum: int) -> str:
    """
    Convert integer to new base system using the given symbols with a
    minimum length filled using leading characters of the lowest value in the
    given charset.

    Parameters
    ----------
    charset : str
        length of this str will be the new base system and characters
        given will be the symbols used.
    input : int
        integer to convert.
    minimum : int
        returned output will be adjusted to this desired length using
        leading characters of the lowest value in charset.

    Returns
    -------
    str
        converted value in given system.
    """
    res = ""
    while input:
        res+=charset[input%len(charset)]
        input//=len(charset)
    return (res[::-1] or charset[0]).rjust(minimum, charset[0])


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
        database connection object.
    root : str
        destination directory for project.
    """
    db_tiles = all_db_tiles(conn)
    cursor = conn.cursor()
    untracked_tiles = 0
    untracked_subregions = 0
    untracked_utms = 0
    for fields in db_tiles:
        if (
        (fields["geotiff_disk"] and
        os.path.isfile(os.path.join(root, fields["geotiff_disk"])) == False) or
        (fields["rat_disk"] and
        os.path.isfile(os.path.join(root, fields["rat_disk"])) == False)
        ):
            cursor.execute("DELETE FROM tiles where tilename = ? RETURNING *",
                           (fields["tilename"],))
            del_tile = cursor.fetchone()
            if del_tile:
                untracked_tiles += 1
                files = ["geotiff_disk", "rat_disk"]
                for file in files:
                    try:
                        if (del_tile[file]
                        and os.path.isfile(os.path.join(root, del_tile[file]))):
                            os.remove(os.path.join(root, del_tile[file]))
                    except (OSError, PermissionError):
                        continue
            cursor.execute("""DELETE FROM vrt_subregion
                            WHERE region NOT IN
                            (SELECT subregion
                             FROM tiles
                             WHERE geotiff_disk is not null
                             AND rat_disk is not null)
                            RETURNING *;""")
            del_subregions = cursor.fetchall()
            untracked_subregions += len(del_subregions)
            for del_subregion in del_subregions:
                files = ["res_2_vrt", "res_2_ovr", "res_4_vrt", "res_4_ovr",
                        "res_8_vrt", "res_8_ovr",
                        "complete_vrt", "complete_ovr"]
                for file in files:
                    try:
                        if (del_subregion[file] and
                        os.path.isfile(os.path.join(root, del_subregion[file]))):
                            os.remove(os.path.join(root, del_subregion[file]))
                    except (OSError, PermissionError):
                        continue
            cursor.execute("""DELETE FROM vrt_utm
                            WHERE utm NOT IN
                            (SELECT utm
                             FROM tiles
                             WHERE geotiff_disk is not null
                             AND rat_disk is not null)
                            RETURNING *;""")
            del_utms = cursor.fetchall()
            untracked_utms += len(del_utms)
            for del_utm in del_utms:
                files = ["utm_vrt", "utm_ovr"]
                for file in files:
                    try:
                        if ((del_utm[file]) and
                        (os.path.isfile(os.path.join(root, del_utm[file])))):
                            os.remove(os.path.join(root, del_utm[file]))
                    except (OSError, PermissionError):
                        continue
            conn.commit()
    return untracked_tiles, untracked_subregions, untracked_utms


def main(root: str,
         desired_area_filename: str = None,
         untrack_missing: bool = False,
         target: str = None
        ) -> [[str],[str]]:
    """
    Track tiles. Download tiles. Update already tracked tiles.

    Parameters
    ----------
    root : str
        destination directory for project.
    desired_area_filename : str
        a gdal compatible file path denoting geometries that reflect the region
        of interest.
    untrack_missing : bool
        if true, files that are missing or deleted will be removed from tracking
        rather than remaining in system to be potentially redownloaded.
    target : str
        the datasource the script will target. only must be specified if it is
        not BlueTopo e.g. 'Modeling'.

    Returns
    -------
    found : list
        tiles downloaded.
    not_found : list
        tiles not downloaded.
    """
    if target is None or target.lower() == "bluetopo":
        target = "BlueTopo"
        geom_prefix = "BlueTopo/_BlueTopo_Tile_Scheme/BlueTopo_Tile_Scheme"
        tile_prefix = "BlueTopo"

    elif target.lower() == "modeling":
        target = "Modeling"
        geom_prefix = "Test-and-Evaluation/Modeling/_Modeling_Tile_Scheme/Modeling_Tile_Scheme"
        tile_prefix = "Test-and-Evaluation/Modeling"

    else:
        raise ValueError(f"Invalid target data: {target}")

    start = datetime.datetime.now()
    print(f"{target}: Beginning work on {root}")
    if not os.path.exists(root):
        os.makedirs(root)

    conn = connect_to_survey_registry(root, target)
    geom_file = get_tessellation(conn, root, geom_prefix, target)

    if untrack_missing:
        untracked_tiles, untracked_sr, untracked_utms = sweep_files(conn, root)
        print(f"Untracked {untracked_tiles} tile(s), "
              f"{untracked_sr} subregion vrt(s), "
              f"{untracked_utms} utm vrt(s)")

    if desired_area_filename:
        if not os.path.isfile(desired_area_filename):
            raise ValueError(f"The geometry {desired_area_filename} for "
                              "determining what to download does not exist.")
        tile_list = get_tile_list(desired_area_filename, geom_file)
        available_tile_count = insert_new(conn, tile_list)
        print(f"Tracking {available_tile_count} available {target} tile(s) "
              f"discovered in a total of {len(tile_list)} intersected tile(s) "
               "with given polygon.")

    upsert_tiles(conn, root, geom_file)
    existing, found, not_found = download_tiles(conn, root, tile_prefix, target)

    lapse = datetime.datetime.now() - start
    print(f"\nOperation complete after {lapse} with {len(existing)} already "
          f"existing tiles, {len(found)} tiles downloaded and {len(not_found)} "
          f"not available on AWS.")
    return found, not_found