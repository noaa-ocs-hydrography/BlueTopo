import collections
import copy
import datetime
import os
import platform
import shutil
import sqlite3
import sys

import numpy as np
from osgeo import gdal

gdal.UseExceptions()
gdal.SetConfigOption("COMPRESS_OVERVIEW", "DEFLATE")
gdal.SetConfigOption("GDAL_NUM_THREADS", "ALL_CPUS")

# refactor duplicate functions

def connect_to_survey_registry_pmn2(project_dir: str, data_source: str) -> sqlite3.Connection:
    """
    Create new or connect to existing SQLite database.

    Parameters
    ----------
    project_dir : str
        destination directory for project.
    data_source : str
        the data source for the project e.g. 'BlueTopo' or 'Modeling'.

    Returns
    -------
    conn : sqlite3.Connection
        connection to SQLite database.
    """
    catalog_fields = {"file": "text", "location": "text", "downloaded": "text"}     
    vrt_subregion_fields = {"region": "text", "utm": "text", 
                            "res_2_subdataset1_vrt": "text", "res_2_subdataset1_ovr": "text", 
                            "res_2_subdataset2_vrt": "text", "res_2_subdataset2_ovr": "text",
                            "res_4_subdataset1_vrt": "text", "res_4_subdataset1_ovr": "text", 
                            "res_4_subdataset2_vrt": "text", "res_4_subdataset2_ovr": "text", 
                            "res_8_subdataset1_vrt": "text", "res_8_subdataset1_ovr": "text", 
                            "res_8_subdataset2_vrt": "text", "res_8_subdataset2_ovr": "text", 
                            "complete_subdataset1_vrt": "text", "complete_subdataset1_ovr": "text",
                            "complete_subdataset2_vrt": "text", "complete_subdataset2_ovr": "text",
                            "built_subdataset1": "integer",
                            "built_subdataset2": "integer"}
    vrt_utm_fields = {"utm": "text", 
                    "utm_subdataset1_vrt": "text", "utm_subdataset1_ovr": "text",
                    "utm_subdataset2_vrt": "text", "utm_subdataset2_ovr": "text",
                    "utm_combined_vrt": "text",
                    "built_subdataset1": "integer",
                    "built_subdataset2": "integer",
                    "built_combined": "integer"}
    vrt_tiles = {"tilename": "text", 
                 "file_link": "text", 
                 "delivered_date": "text", "resolution": "text", 
                 "utm": "text", "subregion": "text", 
                 "file_disk": "text", 
                 "file_sha256_checksum": "text",
                 "file_verified": "text"}
    database_path = os.path.join(project_dir, f"{data_source.lower()}_registry.db")
    conn = None
    try:
        conn = sqlite3.connect(database_path)
        conn.row_factory = sqlite3.Row
    except sqlite3.Error as e:
        print("Failed to establish SQLite database connection.")
        raise e
    if conn is not None:
        try:
            cursor = conn.cursor()
            cursor.execute(
                """
                CREATE TABLE IF NOT EXISTS catalog (
                file text PRIMARY KEY
                );
                """
            )
            cursor.execute(
                """
                CREATE TABLE IF NOT EXISTS vrt_subregion (
                region text PRIMARY KEY
                );
                """
            )
            cursor.execute(
                """
                CREATE TABLE IF NOT EXISTS vrt_utm (
                utm text PRIMARY KEY
                );
                """
            )
            cursor.execute(
                """
                CREATE TABLE IF NOT EXISTS tiles (
                tilename text PRIMARY KEY
                );
                """
            )
            conn.commit()
            cursor.execute("SELECT name FROM pragma_table_info('catalog')")
            tileset_existing_fields = [dict(row)["name"] for row in cursor.fetchall()]
            cursor.execute("SELECT name FROM pragma_table_info('vrt_subregion')")
            vrt_subregion_existing_fields = [dict(row)["name"] for row in cursor.fetchall()]
            cursor.execute("SELECT name FROM pragma_table_info('vrt_utm')")
            vrt_utm_existing_fields = [dict(row)["name"] for row in cursor.fetchall()]
            cursor.execute("SELECT name FROM pragma_table_info('tiles')")
            tiles_existing_fields = [dict(row)["name"] for row in cursor.fetchall()]
            for field in catalog_fields:
                if field not in tileset_existing_fields:
                    cursor.execute(f"ALTER TABLE catalog ADD COLUMN {field} {catalog_fields[field]}")
                    conn.commit()
            for field in vrt_subregion_fields:
                if field not in vrt_subregion_existing_fields:
                    cursor.execute(f"ALTER TABLE vrt_subregion ADD COLUMN {field} {vrt_subregion_fields[field]}")
                    conn.commit()
            for field in vrt_utm_fields:
                if field not in vrt_utm_existing_fields:
                    cursor.execute(f"ALTER TABLE vrt_utm ADD COLUMN {field} {vrt_utm_fields[field]}")
                    conn.commit()
            for field in vrt_tiles:
                if field not in tiles_existing_fields:
                    cursor.execute(f"ALTER TABLE tiles ADD COLUMN {field} {vrt_tiles[field]}")
                    conn.commit()
        except sqlite3.Error as e:
            print("Failed to create SQLite tables.")
            raise e
    return conn


def connect_to_survey_registry_pmn1(project_dir: str, data_source: str) -> sqlite3.Connection:
    """
    Create new or connect to existing SQLite database.

    Parameters
    ----------
    project_dir : str
        destination directory for project.
    data_source : str
        the data source for the project e.g. 'BlueTopo' or 'Modeling'.

    Returns
    -------
    conn : sqlite3.Connection
        connection to SQLite database.
    """
    catalog_fields = {"file": "text", "location": "text", "downloaded": "text"}
    vrt_subregion_fields = {"region": "text", "utm": "text", 
                        "res_2_vrt": "text", "res_2_ovr": "text", 
                        "res_4_vrt": "text", "res_4_ovr": "text", 
                        "res_8_vrt": "text", "res_8_ovr": "text", 
                        "complete_vrt": "text", "complete_ovr": "text",
                        "built": "integer"}
    vrt_utm_fields = {"utm": "text", 
                    "utm_vrt": "text", "utm_ovr": "text",
                    "built": "integer"}
    vrt_tiles = {"tilename": "text", 
                 "file_link": "text", 
                 "delivered_date": "text", "resolution": "text", 
                 "utm": "text", "subregion": "text", 
                 "file_disk": "text", 
                 "file_sha256_checksum": "text",
                 "file_verified": "text"}
    database_path = os.path.join(project_dir, f"{data_source.lower()}_registry.db")
    conn = None
    try:
        conn = sqlite3.connect(database_path)
        conn.row_factory = sqlite3.Row
    except sqlite3.Error as e:
        print("Failed to establish SQLite database connection.")
        raise e
    if conn is not None:
        try:
            cursor = conn.cursor()
            cursor.execute(
                """
                CREATE TABLE IF NOT EXISTS catalog (
                file text PRIMARY KEY
                );
                """
            )
            cursor.execute(
                """
                CREATE TABLE IF NOT EXISTS vrt_subregion (
                region text PRIMARY KEY
                );
                """
            )
            cursor.execute(
                """
                CREATE TABLE IF NOT EXISTS vrt_utm (
                utm text PRIMARY KEY
                );
                """
            )
            cursor.execute(
                """
                CREATE TABLE IF NOT EXISTS tiles (
                tilename text PRIMARY KEY
                );
                """
            )
            conn.commit()
            cursor.execute("SELECT name FROM pragma_table_info('catalog')")
            tileset_existing_fields = [dict(row)["name"] for row in cursor.fetchall()]
            cursor.execute("SELECT name FROM pragma_table_info('vrt_subregion')")
            vrt_subregion_existing_fields = [dict(row)["name"] for row in cursor.fetchall()]
            cursor.execute("SELECT name FROM pragma_table_info('vrt_utm')")
            vrt_utm_existing_fields = [dict(row)["name"] for row in cursor.fetchall()]
            cursor.execute("SELECT name FROM pragma_table_info('tiles')")
            tiles_existing_fields = [dict(row)["name"] for row in cursor.fetchall()]
            for field in catalog_fields:
                if field not in tileset_existing_fields:
                    cursor.execute(f"ALTER TABLE catalog ADD COLUMN {field} {catalog_fields[field]}")
                    conn.commit()
            for field in vrt_subregion_fields:
                if field not in vrt_subregion_existing_fields:
                    cursor.execute(f"ALTER TABLE vrt_subregion ADD COLUMN {field} {vrt_subregion_fields[field]}")
                    conn.commit()
            for field in vrt_utm_fields:
                if field not in vrt_utm_existing_fields:
                    cursor.execute(f"ALTER TABLE vrt_utm ADD COLUMN {field} {vrt_utm_fields[field]}")
                    conn.commit()
            for field in vrt_tiles:
                if field not in tiles_existing_fields:
                    cursor.execute(f"ALTER TABLE tiles ADD COLUMN {field} {vrt_tiles[field]}")
                    conn.commit()
        except sqlite3.Error as e:
            print("Failed to create SQLite tables.")
            raise e
    return conn


def connect_to_survey_registry(project_dir: str, data_source: str) -> sqlite3.Connection:
    """
    Create new or connect to existing SQLite database.

    Parameters
    ----------
    project_dir : str
        destination directory for project.
    data_source : str
        the data source for the project e.g. 'BlueTopo' or 'Modeling'.

    Returns
    -------
    conn : sqlite3.Connection
        connection to SQLite database.
    """
    tileset_fields = {"tilescheme": "text", "location": "text", "downloaded": "text"}
    vrt_subregion_fields = {"region": "text", "utm": "text", "res_2_vrt": "text", "res_2_ovr": "text", "res_4_vrt": "text", "res_4_ovr": "text", "res_8_vrt": "text", "res_8_ovr": "text", "complete_vrt": "text", "complete_ovr": "text", "built": "integer"}
    vrt_utm_fields = {"utm": "text", "utm_vrt": "text", "utm_ovr": "text", "built": "integer"}
    vrt_tiles = {"tilename": "text", "geotiff_link": "text", "rat_link": "text", "delivered_date": "text", "resolution": "text", "utm": "text", "subregion": "text", "geotiff_disk": "text", "rat_disk": "text", "geotiff_sha256_checksum": "text", "rat_sha256_checksum": "text", "geotiff_verified": "text", "rat_verified": "text"}
    database_path = os.path.join(project_dir, f"{data_source.lower()}_registry.db")
    conn = None
    try:
        conn = sqlite3.connect(database_path)
        conn.row_factory = sqlite3.Row
    except sqlite3.Error as e:
        print("Failed to establish SQLite database connection.")
        raise e
    if conn is not None:
        try:
            cursor = conn.cursor()
            cursor.execute(
                """
                CREATE TABLE IF NOT EXISTS tileset (
                tilescheme text PRIMARY KEY
                );
                """
            )
            cursor.execute(
                """
                CREATE TABLE IF NOT EXISTS vrt_subregion (
                region text PRIMARY KEY
                );
                """
            )
            cursor.execute(
                """
                CREATE TABLE IF NOT EXISTS vrt_utm (
                utm text PRIMARY KEY
                );
                """
            )
            cursor.execute(
                """
                CREATE TABLE IF NOT EXISTS tiles (
                tilename text PRIMARY KEY
                );
                """
            )
            conn.commit()
            cursor.execute("SELECT name FROM pragma_table_info('tileset')")
            tileset_existing_fields = [dict(row)["name"] for row in cursor.fetchall()]
            cursor.execute("SELECT name FROM pragma_table_info('vrt_subregion')")
            vrt_subregion_existing_fields = [dict(row)["name"] for row in cursor.fetchall()]
            cursor.execute("SELECT name FROM pragma_table_info('vrt_utm')")
            vrt_utm_existing_fields = [dict(row)["name"] for row in cursor.fetchall()]
            cursor.execute("SELECT name FROM pragma_table_info('tiles')")
            tiles_existing_fields = [dict(row)["name"] for row in cursor.fetchall()]
            for field in tileset_fields:
                if field not in tileset_existing_fields:
                    cursor.execute(f"ALTER TABLE tileset ADD COLUMN {field} {tileset_fields[field]}")
                    conn.commit()
            for field in vrt_subregion_fields:
                if field not in vrt_subregion_existing_fields:
                    cursor.execute(f"ALTER TABLE vrt_subregion ADD COLUMN {field} {vrt_subregion_fields[field]}")
                    conn.commit()
            for field in vrt_utm_fields:
                if field not in vrt_utm_existing_fields:
                    cursor.execute(f"ALTER TABLE vrt_utm ADD COLUMN {field} {vrt_utm_fields[field]}")
                    conn.commit()
            for field in vrt_tiles:
                if field not in tiles_existing_fields:
                    cursor.execute(f"ALTER TABLE tiles ADD COLUMN {field} {vrt_tiles[field]}")
                    conn.commit()
        except sqlite3.Error as e:
            print("Failed to create SQLite tables.")
            raise e
    return conn

def build_sub_vrts_pmn(
    subregion: str,
    subregion_tiles: list,
    project_dir: str,
    data_source: str,
    relative_to_vrt: bool,
) -> dict:
    """
    Build the VRTs of a given subregion.

    Parameters
    ----------
    subregion
        subregion name.
    subregion_tiles
        list of tile records belonging to subregion.
    project_dir
        destination directory for project.
    data_source : str
        the data source for the project e.g. 'BlueTopo' or 'Modeling'.
    relative_to_vrt : bool
        This arg determines if paths of referenced files inside the VRT are relative or absolute paths.

    Returns
    -------
    fields : dict
        holds name of subregion and the paths of its VRT and OVR files.
    """
    fields = {
        "region": subregion["region"],
        "res_2_subdataset1_vrt": None,
        "res_2_subdataset1_ovr": None,
        "res_2_subdataset2_vrt": None,
        "res_2_subdataset2_ovr": None,
        "res_4_subdataset1_vrt": None,
        "res_4_subdataset1_ovr": None,
        "res_4_subdataset2_vrt": None,
        "res_4_subdataset2_ovr": None,
        "res_8_subdataset1_vrt": None,
        "res_8_subdataset1_ovr": None,
        "res_8_subdataset2_vrt": None,
        "res_8_subdataset2_ovr": None,
        "complete_subdataset1_vrt": None,
        "complete_subdataset1_ovr": None,
        "complete_subdataset2_vrt": None,
        "complete_subdataset2_ovr": None,
    }
    rel_dir = os.path.join(f"{data_source}_VRT", subregion["region"])
    subregion_dir = os.path.join(project_dir, rel_dir)
    try:
        if os.path.isdir(subregion_dir):
            shutil.rmtree(subregion_dir)
    except (OSError, PermissionError) as e:
        print(f"Failed to remove older vrt files for {subregion['region']}\n" "Please close all files and attempt again")
        sys.exit(1)
    if not os.path.exists(subregion_dir):
        os.makedirs(subregion_dir)
    resolution_tiles = collections.defaultdict(list)
    for subregion_tile in subregion_tiles:
        resolution_tiles[subregion_tile["resolution"]].append(subregion_tile)
    vrt_subdataset1_list = []
    vrt_subdataset2_list = []
    for res, tiles in resolution_tiles.items():
        print(f"Building {subregion['region']} band {res}...")
        rel_subdataset1_path = os.path.join(rel_dir, subregion["region"] + f"_{res}_BathymetryCoverage.vrt")
        res_subdataset1_vrt = os.path.join(project_dir, rel_subdataset1_path)
        rel_subdataset2_path = os.path.join(rel_dir, subregion["region"] + f"_{res}_QualityOfSurvey.vrt")
        res_subdataset2_vrt = os.path.join(project_dir, rel_subdataset2_path)
        tiffs_subdataset1 = [os.path.join(project_dir, tile["file_disk"]) for tile in tiles]
        tiffs_subdataset2 = []
        for tile in tiles:
            fpath =  os.path.join(project_dir, f'{tile["file_disk"]}').replace("\\", "/")
            if os.path.join(project_dir, f'{tile["file_disk"]}').startswith('/') and os.path.join(project_dir, f'{tile["file_disk"]}').startswith('//') is False:
                tiffs_subdataset2.append('S102:"/' + fpath + '":QualityOfSurvey')
            else:
                tiffs_subdataset2.append('S102:"' + fpath + '":QualityOfSurvey')
        # tiffs_subdataset2 = ['S102:/' + os.path.join(project_dir, f'{tile["file_disk"]}') + ':QualityOfSurvey' for tile in tiles]
        # revisit levels
        if "2" in res:
            create_vrt_pmn(tiffs_subdataset1, res_subdataset1_vrt, [2, 4], relative_to_vrt, subdataset = 1)
            vrt_subdataset1_list.append(res_subdataset1_vrt)
            create_vrt_pmn(tiffs_subdataset2, res_subdataset2_vrt, [2, 4], relative_to_vrt, subdataset = 2)
            vrt_subdataset2_list.append(res_subdataset2_vrt)
            fields["res_2_subdataset1_vrt"] = rel_subdataset1_path
            fields["res_2_subdataset2_vrt"] = rel_subdataset2_path
            if os.path.isfile(os.path.join(project_dir, fields["res_2_subdataset1_vrt"] + ".ovr")):
                fields["res_2_subdataset1_ovr"] = rel_subdataset1_path + ".ovr"
            if os.path.isfile(os.path.join(project_dir, fields["res_2_subdataset2_vrt"] + ".ovr")):
                fields["res_2_subdataset2_ovr"] = rel_subdataset2_path + ".ovr"
        if "4" in res:
            create_vrt_pmn(tiffs_subdataset1, res_subdataset1_vrt, [4, 8], relative_to_vrt, subdataset = 1)
            vrt_subdataset1_list.append(res_subdataset1_vrt)
            create_vrt_pmn(tiffs_subdataset2, res_subdataset2_vrt, [4, 8], relative_to_vrt, subdataset = 2)
            vrt_subdataset2_list.append(res_subdataset2_vrt)
            fields["res_4_subdataset1_vrt"] = rel_subdataset1_path
            fields["res_4_subdataset2_vrt"] = rel_subdataset2_path
            if os.path.isfile(os.path.join(project_dir, fields["res_4_subdataset1_vrt"] + ".ovr")):
                fields["res_4_subdataset1_ovr"] = rel_subdataset1_path + ".ovr"
            if os.path.isfile(os.path.join(project_dir, fields["res_4_subdataset2_vrt"] + ".ovr")):
                fields["res_4_subdataset2_ovr"] = rel_subdataset2_path + ".ovr"
        if "8" in res:
            create_vrt_pmn(tiffs_subdataset1, res_subdataset1_vrt, [8], relative_to_vrt, subdataset = 1)
            vrt_subdataset1_list.append(res_subdataset1_vrt)
            create_vrt_pmn(tiffs_subdataset2, res_subdataset2_vrt, [8], relative_to_vrt, subdataset = 2)
            vrt_subdataset2_list.append(res_subdataset2_vrt)
            fields["res_8_subdataset1_vrt"] = rel_subdataset1_path
            fields["res_8_subdataset2_vrt"] = rel_subdataset2_path
            if os.path.isfile(os.path.join(project_dir, fields["res_8_subdataset1_vrt"] + ".ovr")):
                fields["res_8_subdataset1_ovr"] = rel_subdataset1_path + ".ovr"
            if os.path.isfile(os.path.join(project_dir, fields["res_8_subdataset2_vrt"] + ".ovr")):
                fields["res_8_subdataset2_ovr"] = rel_subdataset2_path + ".ovr"
        if "16" in res:
            vrt_subdataset1_list.extend(tiffs_subdataset1)
            vrt_subdataset2_list.extend(tiffs_subdataset2)
    rel_subdataset1_path = os.path.join(rel_dir, subregion["region"] + "_complete_BathymetryCoverage.vrt")
    complete_subdataset1_vrt = os.path.join(project_dir, rel_subdataset1_path)
    rel_subdataset2_path = os.path.join(rel_dir, subregion["region"] + "_complete_QualityOfSurvey.vrt")
    complete_subdataset2_vrt = os.path.join(project_dir, rel_subdataset2_path)
    create_vrt_pmn(vrt_subdataset1_list, complete_subdataset1_vrt, [16], relative_to_vrt, subdataset = 1)
    create_vrt_pmn(vrt_subdataset2_list, complete_subdataset2_vrt, [16], relative_to_vrt, subdataset = 2)
    fields["complete_subdataset1_vrt"] = rel_subdataset1_path
    if os.path.isfile(os.path.join(project_dir, fields["complete_subdataset1_vrt"] + ".ovr")):
        fields["complete_subdataset1_ovr"] = rel_subdataset1_path + ".ovr"
    fields["complete_subdataset2_vrt"] = rel_subdataset2_path
    if os.path.isfile(os.path.join(project_dir, fields["complete_subdataset2_vrt"] + ".ovr")):
        fields["complete_subdataset2_ovr"] = rel_subdataset2_path + ".ovr"
    return fields

def build_sub_vrts_pmn1(
    subregion: str,
    subregion_tiles: list,
    project_dir: str,
    data_source: str,
    relative_to_vrt: bool,
) -> dict:
    """
    Build the VRTs of a given subregion.

    Parameters
    ----------
    subregion
        subregion name.
    subregion_tiles
        list of tile records belonging to subregion.
    project_dir
        destination directory for project.
    data_source : str
        the data source for the project e.g. 'BlueTopo' or 'Modeling'.
    relative_to_vrt : bool
        This arg determines if paths of referenced files inside the VRT are relative or absolute paths.

    Returns
    -------
    fields : dict
        holds name of subregion and the paths of its VRT and OVR files.
    """
    fields = {
        "region": subregion["region"],
        "res_2_vrt": None,
        "res_2_ovr": None,
        "res_4_vrt": None,
        "res_4_ovr": None,
        "res_8_vrt": None,
        "res_8_ovr": None,
        "complete_vrt": None,
        "complete_ovr": None,
    }
    rel_dir = os.path.join(f"{data_source}_VRT", subregion["region"])
    subregion_dir = os.path.join(project_dir, rel_dir)
    try:
        if os.path.isdir(subregion_dir):
            shutil.rmtree(subregion_dir)
    except (OSError, PermissionError) as e:
        print(f"Failed to remove older vrt files for {subregion['region']}\n" "Please close all files and attempt again")
        sys.exit(1)
    if not os.path.exists(subregion_dir):
        os.makedirs(subregion_dir)
    resolution_tiles = collections.defaultdict(list)
    for subregion_tile in subregion_tiles:
        resolution_tiles[subregion_tile["resolution"]].append(subregion_tile)
    vrt_list = []
    for res, tiles in resolution_tiles.items():
        print(f"Building {subregion['region']} band {res}...")
        rel_path = os.path.join(rel_dir, subregion["region"] + f"_{res}.vrt")
        res_vrt = os.path.join(project_dir, rel_path)
        tiffs = [os.path.join(project_dir, tile["file_disk"]) for tile in tiles]
        # revisit levels
        if "2" in res:
            create_vrt_pmn1(tiffs, res_vrt, [2, 4], relative_to_vrt)
            vrt_list.append(res_vrt)
            fields["res_2_vrt"] = rel_path
            if os.path.isfile(os.path.join(project_dir, fields["res_2_vrt"] + ".ovr")):
                fields["res_2_ovr"] = rel_path + ".ovr"
        if "4" in res:
            create_vrt_pmn1(tiffs, res_vrt, [4, 8], relative_to_vrt)
            vrt_list.append(res_vrt)
            fields["res_4_vrt"] = rel_path
            if os.path.isfile(os.path.join(project_dir, fields["res_4_vrt"] + ".ovr")):
                fields["res_4_ovr"] = rel_path + ".ovr"
        if "8" in res:
            create_vrt_pmn1(tiffs, res_vrt, [8], relative_to_vrt)
            vrt_list.append(res_vrt)
            fields["res_8_vrt"] = rel_path
            if os.path.isfile(os.path.join(project_dir, fields["res_8_vrt"] + ".ovr")):
                fields["res_8_ovr"] = rel_path + ".ovr"
        if "16" in res:
            vrt_list.extend(tiffs)
    rel_path = os.path.join(rel_dir, subregion["region"] + "_complete.vrt")
    complete_vrt = os.path.join(project_dir, rel_path)
    create_vrt_pmn1(vrt_list, complete_vrt, [16], relative_to_vrt)
    fields["complete_vrt"] = rel_path
    if os.path.isfile(os.path.join(project_dir, fields["complete_vrt"] + ".ovr")):
        fields["complete_ovr"] = rel_path + ".ovr"
    return fields


def build_sub_vrts(
    subregion: str,
    subregion_tiles: list,
    project_dir: str,
    data_source: str,
    relative_to_vrt: bool,
) -> dict:
    """
    Build the VRTs of a given subregion.

    Parameters
    ----------
    subregion
        subregion name.
    subregion_tiles
        list of tile records belonging to subregion.
    project_dir
        destination directory for project.
    data_source : str
        the data source for the project e.g. 'BlueTopo' or 'Modeling'.
    relative_to_vrt : bool
        This arg determines if paths of referenced files inside the VRT are relative or absolute paths.

    Returns
    -------
    fields : dict
        holds name of subregion and the paths of its VRT and OVR files.
    """
    fields = {
        "region": subregion["region"],
        "res_2_vrt": None,
        "res_2_ovr": None,
        "res_4_vrt": None,
        "res_4_ovr": None,
        "res_8_vrt": None,
        "res_8_ovr": None,
        "complete_vrt": None,
        "complete_ovr": None,
    }
    rel_dir = os.path.join(f"{data_source}_VRT", subregion["region"])
    subregion_dir = os.path.join(project_dir, rel_dir)
    try:
        if os.path.isdir(subregion_dir):
            shutil.rmtree(subregion_dir)
    except (OSError, PermissionError) as e:
        print(f"Failed to remove older vrt files for {subregion['region']}\n" "Please close all files and attempt again")
        sys.exit(1)
    if not os.path.exists(subregion_dir):
        os.makedirs(subregion_dir)
    resolution_tiles = collections.defaultdict(list)
    for subregion_tile in subregion_tiles:
        resolution_tiles[subregion_tile["resolution"]].append(subregion_tile)
    vrt_list = []
    for res, tiles in resolution_tiles.items():
        print(f"Building {subregion['region']} band {res}...")
        rel_path = os.path.join(rel_dir, subregion["region"] + f"_{res}.vrt")
        res_vrt = os.path.join(project_dir, rel_path)
        tiffs = [os.path.join(project_dir, tile["geotiff_disk"]) for tile in tiles]
        # revisit levels
        if "2" in res:
            create_vrt(tiffs, res_vrt, [2, 4], relative_to_vrt)
            vrt_list.append(res_vrt)
            fields["res_2_vrt"] = rel_path
            if os.path.isfile(os.path.join(project_dir, fields["res_2_vrt"] + ".ovr")):
                fields["res_2_ovr"] = rel_path + ".ovr"
        if "4" in res:
            create_vrt(tiffs, res_vrt, [4, 8], relative_to_vrt)
            vrt_list.append(res_vrt)
            fields["res_4_vrt"] = rel_path
            if os.path.isfile(os.path.join(project_dir, fields["res_4_vrt"] + ".ovr")):
                fields["res_4_ovr"] = rel_path + ".ovr"
        if "8" in res:
            create_vrt(tiffs, res_vrt, [8], relative_to_vrt)
            vrt_list.append(res_vrt)
            fields["res_8_vrt"] = rel_path
            if os.path.isfile(os.path.join(project_dir, fields["res_8_vrt"] + ".ovr")):
                fields["res_8_ovr"] = rel_path + ".ovr"
        if "16" in res:
            vrt_list.extend(tiffs)
    rel_path = os.path.join(rel_dir, subregion["region"] + "_complete.vrt")
    complete_vrt = os.path.join(project_dir, rel_path)
    create_vrt(vrt_list, complete_vrt, [16], relative_to_vrt)
    fields["complete_vrt"] = rel_path
    if os.path.isfile(os.path.join(project_dir, fields["complete_vrt"] + ".ovr")):
        fields["complete_ovr"] = rel_path + ".ovr"
    return fields


def combine_vrts(files: list, vrt_path: str, relative_to_vrt: bool) -> None:
    """
    Build VRT from files.

    Parameters
    ----------
    files
        list of the file paths to include in the vrt.
    vrt_path
        output vrt path.
    levels
        list of overview levels to be built with the vrt.
    relative_to_vrt : bool
        This arg determines if paths of referenced files inside the VRT are relative or absolute paths.
    """
    # not efficient but insignificant
    files = copy.deepcopy(files)
    try:
        if os.path.isfile(vrt_path):
            os.remove(vrt_path)
        if os.path.isfile(vrt_path + ".ovr"):
            os.remove(vrt_path + ".ovr")
    except (OSError, PermissionError) as e:
        print(f"Failed to remove older vrt files for {vrt_path}\n" "Please close all files and attempt again")
        sys.exit(1)
    vrt_options = gdal.BuildVRTOptions(options='-separate -allow_projection_difference', resampleAlg="near", resolution="highest")
    cwd = os.getcwd()
    try:
        os.chdir(os.path.dirname(vrt_path))
        if relative_to_vrt is True:
            for idx in range(len(files)):
                files[idx] = os.path.relpath(files[idx], os.path.dirname(vrt_path))
        relative_vrt_path = os.path.relpath(vrt_path, os.getcwd())
        vrt = gdal.BuildVRT(relative_vrt_path, files, options=vrt_options)
        band1 = vrt.GetRasterBand(1)
        band1.SetDescription("Elevation")
        band2 = vrt.GetRasterBand(2)
        band2.SetDescription("Uncertainty")
        band3 = vrt.GetRasterBand(3)
        band3.SetDescription("QualityOfSurvey")
        vrt = None
    except:
        raise RuntimeError(f"VRT failed to build for {vrt_path}")
    finally:
        os.chdir(cwd)


def create_vrt_pmn(files: list, vrt_path: str, levels: list, relative_to_vrt: bool, subdataset: int) -> None:
    """
    Build VRT from files.

    Parameters
    ----------
    files
        list of the file paths to include in the vrt.
    vrt_path
        output vrt path.
    levels
        list of overview levels to be built with the vrt.
    relative_to_vrt : bool
        This arg determines if paths of referenced files inside the VRT are relative or absolute paths.
    """
    # not efficient but insignificant
    files = copy.deepcopy(files)
    try:
        if os.path.isfile(vrt_path):
            os.remove(vrt_path)
        if os.path.isfile(vrt_path + ".ovr"):
            os.remove(vrt_path + ".ovr")
    except (OSError, PermissionError) as e:
        print(f"Failed to remove older vrt files for {vrt_path}\n" "Please close all files and attempt again")
        sys.exit(1)
    vrt_options = gdal.BuildVRTOptions(options='-allow_projection_difference', resampleAlg="near", resolution="highest")
    cwd = os.getcwd()
    try:
        os.chdir(os.path.dirname(vrt_path))
        if relative_to_vrt is True:
            for idx in range(len(files)):
                if 'S102:' in files[idx]:
                    continue
                else:
                    files[idx] = os.path.relpath(files[idx], os.path.dirname(vrt_path))
        relative_vrt_path = os.path.relpath(vrt_path, os.getcwd())
        vrt = gdal.BuildVRT(relative_vrt_path, files, options=vrt_options)
        if subdataset == 1:
            band1 = vrt.GetRasterBand(1)
            band1.SetDescription("Elevation")
            band2 = vrt.GetRasterBand(2)
            band2.SetDescription("Uncertainty")
        if subdataset == 2:
            band1 = vrt.GetRasterBand(1)
            band1.SetDescription("QualityOfSurvey")
        vrt = None
    except:
        raise RuntimeError(f"VRT failed to build for {vrt_path}")
    finally:
        os.chdir(cwd)
    vrt = gdal.Open(vrt_path, 0)
    vrt.BuildOverviews("NEAREST", levels)
    vrt = None

def create_vrt_pmn1(files: list, vrt_path: str, levels: list, relative_to_vrt: bool) -> None:
    """
    Build VRT from files.

    Parameters
    ----------
    files
        list of the file paths to include in the vrt.
    vrt_path
        output vrt path.
    levels
        list of overview levels to be built with the vrt.
    relative_to_vrt : bool
        This arg determines if paths of referenced files inside the VRT are relative or absolute paths.
    """
    # not efficient but insignificant
    files = copy.deepcopy(files)
    try:
        if os.path.isfile(vrt_path):
            os.remove(vrt_path)
        if os.path.isfile(vrt_path + ".ovr"):
            os.remove(vrt_path + ".ovr")
    except (OSError, PermissionError) as e:
        print(f"Failed to remove older vrt files for {vrt_path}\n" "Please close all files and attempt again")
        sys.exit(1)
    vrt_options = gdal.BuildVRTOptions(options='-allow_projection_difference', resampleAlg="near", resolution="highest")
    cwd = os.getcwd()
    try:
        os.chdir(os.path.dirname(vrt_path))
        if relative_to_vrt is True:
            for idx in range(len(files)):
                files[idx] = os.path.relpath(files[idx], os.path.dirname(vrt_path))
        relative_vrt_path = os.path.relpath(vrt_path, os.getcwd())
        vrt = gdal.BuildVRT(relative_vrt_path, files, options=vrt_options)
        band1 = vrt.GetRasterBand(1)
        band1.SetDescription("Elevation")
        band2 = vrt.GetRasterBand(2)
        band2.SetDescription("Uncertainty")
        vrt = None
    except:
        raise RuntimeError(f"VRT failed to build for {vrt_path}")
    finally:
        os.chdir(cwd)
    vrt = gdal.Open(vrt_path, 0)
    vrt.BuildOverviews("NEAREST", levels)
    vrt = None

def create_vrt(files: list, vrt_path: str, levels: list, relative_to_vrt: bool) -> None:
    """
    Build VRT from files.

    Parameters
    ----------
    files
        list of the file paths to include in the vrt.
    vrt_path
        output vrt path.
    levels
        list of overview levels to be built with the vrt.
    relative_to_vrt : bool
        This arg determines if paths of referenced files inside the VRT are relative or absolute paths.
    """
    # not efficient but insignificant
    files = copy.deepcopy(files)
    try:
        if os.path.isfile(vrt_path):
            os.remove(vrt_path)
        if os.path.isfile(vrt_path + ".ovr"):
            os.remove(vrt_path + ".ovr")
    except (OSError, PermissionError) as e:
        print(f"Failed to remove older vrt files for {vrt_path}\n" "Please close all files and attempt again")
        sys.exit(1)
    vrt_options = gdal.BuildVRTOptions(options='-allow_projection_difference', resampleAlg="near", resolution="highest")
    cwd = os.getcwd()
    try:
        os.chdir(os.path.dirname(vrt_path))
        if relative_to_vrt is True:
            for idx in range(len(files)):
                files[idx] = os.path.relpath(files[idx], os.path.dirname(vrt_path))
        relative_vrt_path = os.path.relpath(vrt_path, os.getcwd())
        vrt = gdal.BuildVRT(relative_vrt_path, files, options=vrt_options)
        band1 = vrt.GetRasterBand(1)
        band1.SetDescription("Elevation")
        band2 = vrt.GetRasterBand(2)
        band2.SetDescription("Uncertainty")
        band3 = vrt.GetRasterBand(3)
        band3.SetDescription("Contributor")
        vrt = None
    except:
        raise RuntimeError(f"VRT failed to build for {vrt_path}")
    finally:
        os.chdir(cwd)
    vrt = gdal.Open(vrt_path, 0)
    vrt.BuildOverviews("NEAREST", levels)
    vrt = None


def add_vrt_rat_pmn(conn: sqlite3.Connection, utm: str, project_dir: str, vrt_path: str, data_source: str) -> None:
    """
    Create a raster attribute table for the VRT.

    Parameters
    ----------
    conn : sqlite3.Connection
        database connection object.
    utm : str
        utm zone of the VRT.
    project_dir : str
        destination directory for project.
    vrt_path : str
        path to the VRT to which to add the raster attribute table.
    data_source : str
        The NBS offers various products to different end-users. Some are available publicly.
        Use this argument to identify which product you want. BlueTopo is the default.
    """
    expected_fields = dict(
        value=[int, gdal.GFU_MinMax],
        count=[int, gdal.GFU_PixelCount],
        data_assessment=[int, gdal.GFU_Generic],
        feature_least_depth=[float, gdal.GFU_Generic],
        significant_features=[float, gdal.GFU_Generic],
        feature_size=[float, gdal.GFU_Generic],
        coverage=[int, gdal.GFU_Generic],
        bathy_coverage=[int, gdal.GFU_Generic],
        horizontal_uncert_fixed=[float, gdal.GFU_Generic],
        horizontal_uncert_var=[float, gdal.GFU_Generic],
        vertical_uncert_fixed=[float, gdal.GFU_Generic],
        vertical_uncert_var=[float, gdal.GFU_Generic],
        license_name=[str, gdal.GFU_Generic],
        license_url=[str, gdal.GFU_Generic],
        source_survey_id=[str, gdal.GFU_Generic],
        source_institution=[str, gdal.GFU_Generic],
        survey_date_start=[str, gdal.GFU_Generic],
        survey_date_end=[str, gdal.GFU_Generic],
    )
    if data_source.lower() == "hsd":
        expected_fields["catzoc"] = [int, gdal.GFU_Generic]
        expected_fields["supercession_score"] = [float, gdal.GFU_Generic]
        expected_fields["decay_score"] = [float, gdal.GFU_Generic]
        expected_fields["unqualified"] = [int, gdal.GFU_Generic]
        expected_fields["sensitive"] = [int, gdal.GFU_Generic]
    # refactor later
    if data_source.lower() in ['s102v22']:
        expected_fields = dict(
            value=[int, gdal.GFU_MinMax],
            data_assessment=[int, gdal.GFU_Generic],
            feature_least_depth=[float, gdal.GFU_Generic],
            significant_features=[float, gdal.GFU_Generic],
            feature_size=[str, gdal.GFU_Generic],
            # ?
            feature_size_var=[int, gdal.GFU_Generic],
            coverage=[int, gdal.GFU_Generic],
            bathy_coverage=[int, gdal.GFU_Generic],
            horizontal_uncert_fixed=[float, gdal.GFU_Generic],
            horizontal_uncert_var=[float, gdal.GFU_Generic],
            survey_date_start=[str, gdal.GFU_Generic],
            survey_date_end=[str, gdal.GFU_Generic],
            source_survey_id=[str, gdal.GFU_Generic],
            source_institution=[str, gdal.GFU_Generic],
            # ?
            bathymetric_uncertainty_type=[int, gdal.GFU_Generic],
        )
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM tiles WHERE utm = ?", (utm,))
    exp_fields = list(expected_fields.keys())
    tiles = [dict(row) for row in cursor.fetchall()]
    surveys = []
    for tile in tiles:
        if tile['file_disk'] is None or os.path.isfile(os.path.join(project_dir, tile["file_disk"])) is False:
            continue
        gtiff = os.path.join(project_dir, tile["file_disk"]).replace('\\', '/')
        if os.path.isfile(gtiff) is False:
            continue
        # rat_file = os.path.join(project_dir, tile["rat_disk"])
        # if os.path.isfile(rat_file) is False and data_source.lower() != 's102v22':
        #     continue
        if data_source.lower() != 's102v22':
            ds = gdal.Open(gtiff)
            contrib = ds.GetRasterBand(3)
            rat_n = contrib.GetDefaultRAT()
            for col in range(rat_n.GetColumnCount()):
                if exp_fields[col] != rat_n.GetNameOfCol(col).lower():
                    raise ValueError("Unexpected field order")
        else:
            ds = gdal.Open(f'S102:"{gtiff}":QualityOfSurvey')
            contrib = ds.GetRasterBand(1)
            rat_n = contrib.GetDefaultRAT()
        for row in range(rat_n.GetRowCount()):
            exist = False
            for survey in surveys:
                if survey[0] == rat_n.GetValueAsString(row, 0):
                    survey[1] = int(survey[1]) + rat_n.GetValueAsInt(row, 1)
                    # this is the count field
                    # GFU_PixelCount usage has support as int dtype in some
                    # software so avoiding changing it to python float (double)
                    # this is a temp solution to avoid overflow error which can
                    # occur with generalization in vrts of extreme coverage
                    if survey[1] > 2147483647:
                        survey[1] = 2147483647
                    exist = True
                    break
            if exist:
                continue
            curr = []
            for col in range(rat_n.GetColumnCount()):
                entry_val = rat_n.GetValueAsString(row, col)
                # test removal
                if rat_n.GetNameOfCol(col).lower() in ['feature_size_var', 'bathymetric_uncertainty_type']:
                    entry_val = 0
                curr.append(entry_val)
            surveys.append(curr)
    rat = gdal.RasterAttributeTable()
    for entry in expected_fields:
        field_type, usage = expected_fields[entry]
        if field_type == str:
            col_type = gdal.GFT_String
        elif field_type == int:
            col_type = gdal.GFT_Integer
        elif field_type == float:
            col_type = gdal.GFT_Real
        else:
            raise TypeError("Unknown data type submitted for gdal raster attribute table.")
        rat.CreateColumn(entry, col_type, usage)
    rat.SetRowCount(len(surveys))
    for row_idx, survey in enumerate(surveys):
        for col_idx, entry in enumerate(expected_fields):
            field_type, usage = expected_fields[entry]
            if field_type == str:
                rat.SetValueAsString(row_idx, col_idx, survey[col_idx])
            elif field_type == int:
                rat.SetValueAsInt(row_idx, col_idx, int(survey[col_idx]))
            elif field_type == float:
                rat.SetValueAsDouble(row_idx, col_idx, float(survey[col_idx]))
    vrt_ds = gdal.Open(vrt_path, 1)
    contributor_band = vrt_ds.GetRasterBand(3)
    contributor_band.SetDefaultRAT(rat)


def add_vrt_rat(conn: sqlite3.Connection, utm: str, project_dir: str, vrt_path: str, data_source: str) -> None:
    """
    Create a raster attribute table for the VRT.

    Parameters
    ----------
    conn : sqlite3.Connection
        database connection object.
    utm : str
        utm zone of the VRT.
    project_dir : str
        destination directory for project.
    vrt_path : str
        path to the VRT to which to add the raster attribute table.
    data_source : str
        The NBS offers various products to different end-users. Some are available publicly.
        Use this argument to identify which product you want. BlueTopo is the default.
    """
    expected_fields = dict(
        value=[int, gdal.GFU_MinMax],
        count=[int, gdal.GFU_PixelCount],
        data_assessment=[int, gdal.GFU_Generic],
        feature_least_depth=[float, gdal.GFU_Generic],
        significant_features=[float, gdal.GFU_Generic],
        feature_size=[float, gdal.GFU_Generic],
        coverage=[int, gdal.GFU_Generic],
        bathy_coverage=[int, gdal.GFU_Generic],
        horizontal_uncert_fixed=[float, gdal.GFU_Generic],
        horizontal_uncert_var=[float, gdal.GFU_Generic],
        vertical_uncert_fixed=[float, gdal.GFU_Generic],
        vertical_uncert_var=[float, gdal.GFU_Generic],
        license_name=[str, gdal.GFU_Generic],
        license_url=[str, gdal.GFU_Generic],
        source_survey_id=[str, gdal.GFU_Generic],
        source_institution=[str, gdal.GFU_Generic],
        survey_date_start=[str, gdal.GFU_Generic],
        survey_date_end=[str, gdal.GFU_Generic],
    )
    if data_source.lower() == "hsd":
        expected_fields['catzoc'] = [int, gdal.GFU_Generic]
        expected_fields['supercession_score'] = [float, gdal.GFU_Generic]
        expected_fields['decay_score'] = [float, gdal.GFU_Generic]
        expected_fields['unqualified'] = [int, gdal.GFU_Generic]
        expected_fields['sensitive'] = [int, gdal.GFU_Generic]
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM tiles WHERE utm = ?", (utm,))
    exp_fields = list(expected_fields.keys())
    tiles = [dict(row) for row in cursor.fetchall()]
    surveys = []
    for tile in tiles:
        gtiff = os.path.join(project_dir, tile["geotiff_disk"])
        if os.path.isfile(gtiff) is False:
            continue
        rat_file = os.path.join(project_dir, tile["rat_disk"])
        if os.path.isfile(rat_file) is False:
            continue
        ds = gdal.Open(gtiff)
        contrib = ds.GetRasterBand(3)
        rat_n = contrib.GetDefaultRAT()
        for col in range(rat_n.GetColumnCount()):
            if exp_fields[col] != rat_n.GetNameOfCol(col).lower():
                raise ValueError("Unexpected field order")
        for row in range(rat_n.GetRowCount()):
            exist = False
            for survey in surveys:
                if survey[0] == rat_n.GetValueAsString(row, 0):
                    survey[1] = int(survey[1]) + rat_n.GetValueAsInt(row, 1)
                    # this is the count field
                    # GFU_PixelCount usage has support as int dtype in some
                    # software so avoiding changing it to python float (double)
                    # this is a temp solution to avoid overflow error which can
                    # occur with generalization in vrts of extreme coverage
                    if survey[1] > 2147483647:
                        survey[1] = 2147483647
                    exist = True
                    break
            if exist:
                continue
            curr = []
            for col in range(rat_n.GetColumnCount()):
                curr.append(rat_n.GetValueAsString(row, col))
            surveys.append(curr)
    rat = gdal.RasterAttributeTable()
    for entry in expected_fields:
        field_type, usage = expected_fields[entry]
        if field_type == str:
            col_type = gdal.GFT_String
        elif field_type == int:
            col_type = gdal.GFT_Integer
        elif field_type == float:
            col_type = gdal.GFT_Real
        else:
            raise TypeError("Unknown data type submitted for gdal raster attribute table.")
        rat.CreateColumn(entry, col_type, usage)
    rat.SetRowCount(len(surveys))
    for row_idx, survey in enumerate(surveys):
        for col_idx, entry in enumerate(expected_fields):
            field_type, usage = expected_fields[entry]
            if field_type == str:
                rat.SetValueAsString(row_idx, col_idx, survey[col_idx])
            elif field_type == int:
                rat.SetValueAsInt(row_idx, col_idx, int(survey[col_idx]))
            elif field_type == float:
                rat.SetValueAsDouble(row_idx, col_idx, float(survey[col_idx]))
    vrt_ds = gdal.Open(vrt_path, 1)
    contributor_band = vrt_ds.GetRasterBand(3)
    contributor_band.SetDefaultRAT(rat)


def select_tiles_by_subregion_pmn(project_dir: str, conn: sqlite3.Connection, subregion: str) -> list:
    """
    Retrieve all tile records with files in the given subregion.

    Parameters
    ----------
    project_dir
        destination directory for project.
    conn : sqlite3.Connection
        database connection object.
    subregion : str
        subregion name.

    Returns
    -------
    existing_tiles : list
        list of tile records.
    """
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM tiles WHERE subregion = ?", (subregion,))
    tiles = [dict(row) for row in cursor.fetchall()]
    existing_tiles = [tile for tile in tiles if tile["file_disk"] and os.path.isfile(os.path.join(project_dir, tile["file_disk"]))]
    if len(tiles) - len(existing_tiles) != 0:
        print(f"Did not find the files for {len(tiles) - len(existing_tiles)} " f"registered tile(s) in subregion {subregion}. " "Run fetch_tiles to retrieve files " "or correct the directory path if incorrect.")
    return existing_tiles


def select_tiles_by_subregion(project_dir: str, conn: sqlite3.Connection, subregion: str) -> list:
    """
    Retrieve all tile records with files in the given subregion.

    Parameters
    ----------
    project_dir
        destination directory for project.
    conn : sqlite3.Connection
        database connection object.
    subregion : str
        subregion name.

    Returns
    -------
    existing_tiles : list
        list of tile records.
    """
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM tiles WHERE subregion = ?", (subregion,))
    tiles = [dict(row) for row in cursor.fetchall()]
    existing_tiles = [tile for tile in tiles if tile["geotiff_disk"] and tile["rat_disk"] and os.path.isfile(os.path.join(project_dir, tile["geotiff_disk"])) and os.path.isfile(os.path.join(project_dir, tile["rat_disk"]))]
    if len(tiles) - len(existing_tiles) != 0:
        print(f"Did not find the files for {len(tiles) - len(existing_tiles)} " f"registered tile(s) in subregion {subregion}. " "Run fetch_tiles to retrieve files " "or correct the directory path if incorrect.")
    return existing_tiles


def select_subregions_by_utm_pmn(project_dir: str, conn: sqlite3.Connection, utm: str) -> list:
    """
    Retrieve all subregion records with files in the given UTM.

    Parameters
    ----------
    project_dir
        destination directory for project.
    conn : sqlite3.Connection
        database connection object.
    utm : str
        UTM zone.

    Returns
    -------
    subregions : list
        list of subregion records in UTM zone.
    """
    cursor = conn.cursor()
    cursor.execute(
        """
        SELECT * FROM vrt_subregion
        WHERE utm = ? AND built_subdataset1 = 1 AND built_subdataset2 = 1
        """,
        (utm,),
    )
    subregions = [dict(row) for row in cursor.fetchall()]
    for s in subregions:
        if (
            (s["res_2_subdataset1_vrt"] and not os.path.isfile(os.path.join(project_dir, s["res_2_subdataset1_vrt"])))
            or (s["res_2_subdataset1_ovr"] and not os.path.isfile(os.path.join(project_dir, s["res_2_subdataset1_ovr"])))

            or (s["res_2_subdataset2_vrt"] and not os.path.isfile(os.path.join(project_dir, s["res_2_subdataset2_vrt"])))
            or (s["res_2_subdataset2_ovr"] and not os.path.isfile(os.path.join(project_dir, s["res_2_subdataset2_ovr"])))

            or (s["res_4_subdataset1_vrt"] and not os.path.isfile(os.path.join(project_dir, s["res_4_subdataset1_vrt"])))
            or (s["res_4_subdataset1_ovr"] and not os.path.isfile(os.path.join(project_dir, s["res_4_subdataset1_ovr"])))

            or (s["res_4_subdataset2_vrt"] and not os.path.isfile(os.path.join(project_dir, s["res_4_subdataset2_vrt"])))
            or (s["res_4_subdataset2_ovr"] and not os.path.isfile(os.path.join(project_dir, s["res_4_subdataset2_ovr"])))

            or (s["res_8_subdataset1_vrt"] and not os.path.isfile(os.path.join(project_dir, s["res_8_subdataset1_vrt"])))
            or (s["res_8_subdataset1_ovr"] and not os.path.isfile(os.path.join(project_dir, s["res_8_subdataset1_ovr"])))

            or (s["res_8_subdataset2_vrt"] and not os.path.isfile(os.path.join(project_dir, s["res_8_subdataset2_vrt"])))
            or (s["res_8_subdataset2_ovr"] and not os.path.isfile(os.path.join(project_dir, s["res_8_subdataset2_ovr"])))

            or (s["complete_subdataset1_vrt"] is None or not os.path.isfile(os.path.join(project_dir, s["complete_subdataset1_vrt"])))
            or (s["complete_subdataset1_ovr"] is None or not os.path.isfile(os.path.join(project_dir, s["complete_subdataset1_ovr"])))

            or (s["complete_subdataset2_vrt"] is None or not os.path.isfile(os.path.join(project_dir, s["complete_subdataset2_vrt"])))
            or (s["complete_subdataset2_ovr"] is None or not os.path.isfile(os.path.join(project_dir, s["complete_subdataset2_ovr"])))

        ):
            raise RuntimeError(f"Subregion VRT files missing for {s['utm']}. Please rerun.")
    return subregions


def select_subregions_by_utm(project_dir: str, conn: sqlite3.Connection, utm: str) -> list:
    """
    Retrieve all subregion records with files in the given UTM.

    Parameters
    ----------
    project_dir
        destination directory for project.
    conn : sqlite3.Connection
        database connection object.
    utm : str
        UTM zone.

    Returns
    -------
    subregions : list
        list of subregion records in UTM zone.
    """
    cursor = conn.cursor()
    cursor.execute(
        """
        SELECT * FROM vrt_subregion
        WHERE utm = ? AND built = 1
        """,
        (utm,),
    )
    subregions = [dict(row) for row in cursor.fetchall()]
    for s in subregions:
        if (
            (s["res_2_vrt"] and not os.path.isfile(os.path.join(project_dir, s["res_2_vrt"])))
            or (s["res_2_ovr"] and not os.path.isfile(os.path.join(project_dir, s["res_2_ovr"])))
            or (s["res_4_vrt"] and not os.path.isfile(os.path.join(project_dir, s["res_4_vrt"])))
            or (s["res_4_ovr"] and not os.path.isfile(os.path.join(project_dir, s["res_4_ovr"])))
            or (s["res_8_vrt"] and not os.path.isfile(os.path.join(project_dir, s["res_8_vrt"])))
            or (s["res_8_ovr"] and not os.path.isfile(os.path.join(project_dir, s["res_8_ovr"])))
            or (s["complete_vrt"] is None or not os.path.isfile(os.path.join(project_dir, s["complete_vrt"])))
            or (s["complete_ovr"] is None or not os.path.isfile(os.path.join(project_dir, s["complete_ovr"])))
        ):
            raise RuntimeError(f"Subregion VRT files missing for {s['utm']}. Please rerun.")
    return subregions

def select_unbuilt_subregions_pmn(conn: sqlite3.Connection) -> list:
    """
    Retrieve all unbuilt subregion records.

    Parameters
    ----------
    conn : sqlite3.Connection
        database connection object.

    Returns
    -------
    subregions : list
        list of unbuilt subregion records.
    """
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM vrt_subregion WHERE built_subdataset1 = 0 or built_subdataset2 = 0")
    subregions = [dict(row) for row in cursor.fetchall()]
    return subregions


def select_unbuilt_subregions(conn: sqlite3.Connection) -> list:
    """
    Retrieve all unbuilt subregion records.

    Parameters
    ----------
    conn : sqlite3.Connection
        database connection object.

    Returns
    -------
    subregions : list
        list of unbuilt subregion records.
    """
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM vrt_subregion WHERE built = 0")
    subregions = [dict(row) for row in cursor.fetchall()]
    return subregions


def select_unbuilt_utms_pmn(conn: sqlite3.Connection) -> list:
    """
    Retrieve all unbuilt utm records.

    Parameters
    ----------
    conn : sqlite3.Connection
        database connection object.

    Returns
    -------
    utms : list
        list of unbuilt utm records.
    """
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM vrt_utm WHERE built_subdataset1 = 0 or built_subdataset2 = 0")
    utms = [dict(row) for row in cursor.fetchall()]
    return utms



def select_unbuilt_utms(conn: sqlite3.Connection) -> list:
    """
    Retrieve all unbuilt utm records.

    Parameters
    ----------
    conn : sqlite3.Connection
        database connection object.

    Returns
    -------
    utms : list
        list of unbuilt utm records.
    """
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM vrt_utm WHERE built = 0")
    utms = [dict(row) for row in cursor.fetchall()]
    return utms


def update_subregion_pmn(conn: sqlite3.Connection, fields: dict) -> None:
    """
    Update subregion records with given path values.

    Parameters
    ----------
    conn : sqlite3.Connection
        database connection object.
    fields : dict
        dictionary with the name of the subregion and paths for its associated
        VRT and OVR files.
    """
    cursor = conn.cursor()
    cursor.execute(
        """UPDATE vrt_subregion
                      SET res_2_subdataset1_vrt = ?, res_2_subdataset1_ovr = ?, 
                      res_2_subdataset2_vrt = ?, res_2_subdataset2_ovr = ?, 
                      res_4_subdataset1_vrt = ?, res_4_subdataset1_ovr = ?, 
                      res_4_subdataset2_vrt = ?, res_4_subdataset2_ovr = ?, 
                      res_8_subdataset1_vrt = ?, res_8_subdataset1_ovr = ?, 
                      res_8_subdataset2_vrt = ?, res_8_subdataset2_ovr = ?, 
                      complete_subdataset1_vrt = ?, complete_subdataset1_ovr = ?, 
                      complete_subdataset2_vrt = ?, complete_subdataset2_ovr = ?,
                      built_subdataset1 = 1,
                      built_subdataset2 = 1
                      WHERE region = ?""",
        (
            fields["res_2_subdataset1_vrt"],
            fields["res_2_subdataset1_ovr"],
            fields["res_2_subdataset2_vrt"],
            fields["res_2_subdataset2_ovr"],
            fields["res_4_subdataset1_vrt"],
            fields["res_4_subdataset1_ovr"],
            fields["res_4_subdataset2_vrt"],
            fields["res_4_subdataset2_ovr"],
            fields["res_8_subdataset1_vrt"],
            fields["res_8_subdataset1_ovr"],
            fields["res_8_subdataset2_vrt"],
            fields["res_8_subdataset2_ovr"],

            fields["complete_subdataset1_vrt"],
            fields["complete_subdataset1_ovr"],

            fields["complete_subdataset2_vrt"],
            fields["complete_subdataset2_ovr"],

            fields["region"],
        ),
    )
    conn.commit()


def update_subregion(conn: sqlite3.Connection, fields: dict) -> None:
    """
    Update subregion records with given path values.

    Parameters
    ----------
    conn : sqlite3.Connection
        database connection object.
    fields : dict
        dictionary with the name of the subregion and paths for its associated
        VRT and OVR files.
    """
    cursor = conn.cursor()
    cursor.execute(
        """UPDATE vrt_subregion
                      SET res_2_vrt = ?, res_2_ovr = ?, res_4_vrt = ?,
                      res_4_ovr = ?, res_8_vrt = ?, res_8_ovr = ?,
                      complete_vrt = ?, complete_ovr = ?, built = 1
                      WHERE region = ?""",
        (
            fields["res_2_vrt"],
            fields["res_2_ovr"],
            fields["res_4_vrt"],
            fields["res_4_ovr"],
            fields["res_8_vrt"],
            fields["res_8_ovr"],
            fields["complete_vrt"],
            fields["complete_ovr"],
            fields["region"],
        ),
    )
    conn.commit()


def update_utm_pmn(conn: sqlite3.Connection, fields: dict) -> None:
    """
    Update utm records with given path values.

    Parameters
    ----------
    conn : sqlite3.Connection
        database connection object.
    fields : dict
        dictionary with the name of the UTM zone and paths for its associated
        VRT and OVR files.
    """
    cursor = conn.cursor()
    cursor.execute(
        """UPDATE vrt_utm
                      SET 
                      utm_subdataset1_vrt = ?, utm_subdataset1_ovr = ?, 
                      utm_subdataset2_vrt = ?, utm_subdataset2_ovr = ?, 
                      utm_combined_vrt = ?,
                      built_subdataset1 = 1,
                      built_subdataset2 = 1,
                      built_combined = 1
                      WHERE utm = ?""",
        (
            fields["utm_subdataset1_vrt"],
            fields["utm_subdataset1_ovr"],
            fields["utm_subdataset2_vrt"],
            fields["utm_subdataset2_ovr"],
            fields["utm_combined_vrt"],
            fields["utm"],
        ),
    )
    conn.commit()


def update_utm(conn: sqlite3.Connection, fields: dict) -> None:
    """
    Update utm records with given path values.

    Parameters
    ----------
    conn : sqlite3.Connection
        database connection object.
    fields : dict
        dictionary with the name of the UTM zone and paths for its associated
        VRT and OVR files.
    """
    cursor = conn.cursor()
    cursor.execute(
        """UPDATE vrt_utm
                      SET utm_vrt = ?, utm_ovr = ?, built = 1
                      WHERE utm = ?""",
        (
            fields["utm_vrt"],
            fields["utm_ovr"],
            fields["utm"],
        ),
    )
    conn.commit()


def missing_subregions_pmn(project_dir: str, conn: sqlite3.Connection) -> int:
    """
    Confirm built subregions's associated VRT and OVR files exists.
    If the files do not exist, then change the subregion record to unbuilt.

    Parameters
    ----------
    project_dir
        destination directory for project.
    conn : sqlite3.Connection
        database connection object.

    Returns
    -------
    missing_subregion_count : int
        count of subregions with missing files.
    """
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM vrt_subregion WHERE built_subdataset1 = 1 or built_subdataset2 = 1")
    subregions = [dict(row) for row in cursor.fetchall()]
    missing_subregion_count = 0
    # todo comparison against tiles table to know res vrts exist where expected
    for s in subregions:
        if (
            (s["res_2_subdataset1_vrt"] and not os.path.isfile(os.path.join(project_dir, s["res_2_subdataset1_vrt"])))
            or (s["res_2_subdataset1_ovr"] and not os.path.isfile(os.path.join(project_dir, s["res_2_subdataset1_ovr"])))
            or (s["res_2_subdataset2_vrt"] and not os.path.isfile(os.path.join(project_dir, s["res_2_subdataset2_vrt"])))
            or (s["res_2_subdataset2_ovr"] and not os.path.isfile(os.path.join(project_dir, s["res_2_subdataset2_ovr"])))
            or (s["res_4_subdataset1_vrt"] and not os.path.isfile(os.path.join(project_dir, s["res_4_subdataset1_vrt"])))
            or (s["res_4_subdataset1_ovr"] and not os.path.isfile(os.path.join(project_dir, s["res_4_subdataset1_ovr"])))
            or (s["res_4_subdataset2_vrt"] and not os.path.isfile(os.path.join(project_dir, s["res_4_subdataset2_vrt"])))
            or (s["res_4_subdataset2_ovr"] and not os.path.isfile(os.path.join(project_dir, s["res_4_subdataset2_ovr"])))
            or (s["res_8_subdataset1_vrt"] and not os.path.isfile(os.path.join(project_dir, s["res_8_subdataset1_vrt"])))
            or (s["res_8_subdataset1_ovr"] and not os.path.isfile(os.path.join(project_dir, s["res_8_subdataset1_ovr"])))
            or (s["res_8_subdataset2_vrt"] and not os.path.isfile(os.path.join(project_dir, s["res_8_subdataset2_vrt"])))
            or (s["res_8_subdataset2_ovr"] and not os.path.isfile(os.path.join(project_dir, s["res_8_subdataset2_ovr"])))
            or (s["complete_subdataset1_vrt"] is None or not os.path.isfile(os.path.join(project_dir, s["complete_subdataset1_vrt"])))
            or (s["complete_subdataset1_ovr"] is None or not os.path.isfile(os.path.join(project_dir, s["complete_subdataset1_ovr"])))
            or (s["complete_subdataset2_vrt"] is None or not os.path.isfile(os.path.join(project_dir, s["complete_subdataset2_vrt"])))
            or (s["complete_subdataset2_ovr"] is None or not os.path.isfile(os.path.join(project_dir, s["complete_subdataset2_ovr"])))
        ):
            missing_subregion_count += 1
            cursor.execute(
                """UPDATE vrt_subregion
                           SET res_2_subdataset1_vrt = ?, res_2_subdataset1_ovr = ?, 
                           res_2_subdataset2_vrt = ?, res_2_subdataset2_ovr = ?,

                           res_4_subdataset1_vrt = ?, res_4_subdataset1_ovr = ?, 
                           res_4_subdataset2_vrt = ?, res_4_subdataset2_ovr = ?, 

                           res_8_subdataset1_vrt = ?, res_8_subdataset1_ovr = ?, 
                           res_8_subdataset2_vrt = ?, res_8_subdataset2_ovr = ?, 

                           complete_subdataset1_vrt = ?, complete_subdataset1_ovr = ?, 
                           complete_subdataset2_vrt = ?, complete_subdataset2_ovr = ?, 

                           built_subdataset1 = 0,
                           built_subdataset2 = 0

                           WHERE region = ?""",
                (
                    None,
                    None,
                    None,
                    None,
                    None,
                    None,
                    None,
                    None,
                    None,
                    None,
                    None,
                    None,
                    None,
                    None,
                    None,
                    None,
                    s["region"],
                ),
            )
            cursor.execute(
                """UPDATE vrt_utm
                              SET utm_subdataset1_vrt = ?, utm_subdataset1_ovr = ?, 
                              utm_subdataset2_vrt = ?, utm_subdataset2_ovr = ?, 

                              utm_combined_vrt = ?,
                              
                              built_subdataset1 = 0,
                              built_subdataset2 = 0,
                              built_combined = 0

                              WHERE utm = ?""",
                (
                    None,
                    None,
                    None,
                    None,
                    None,
                    s["utm"],
                ),
            )
            conn.commit()
    return missing_subregion_count



def missing_subregions(project_dir: str, conn: sqlite3.Connection) -> int:
    """
    Confirm built subregions's associated VRT and OVR files exists.
    If the files do not exist, then change the subregion record to unbuilt.

    Parameters
    ----------
    project_dir
        destination directory for project.
    conn : sqlite3.Connection
        database connection object.

    Returns
    -------
    missing_subregion_count : int
        count of subregions with missing files.
    """
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM vrt_subregion WHERE built = 1")
    subregions = [dict(row) for row in cursor.fetchall()]
    missing_subregion_count = 0
    # todo comparison against tiles table to know res vrts exist where expected
    for s in subregions:
        if (
            (s["res_2_vrt"] and not os.path.isfile(os.path.join(project_dir, s["res_2_vrt"])))
            or (s["res_2_ovr"] and not os.path.isfile(os.path.join(project_dir, s["res_2_ovr"])))
            or (s["res_4_vrt"] and not os.path.isfile(os.path.join(project_dir, s["res_4_vrt"])))
            or (s["res_4_ovr"] and not os.path.isfile(os.path.join(project_dir, s["res_4_ovr"])))
            or (s["res_8_vrt"] and not os.path.isfile(os.path.join(project_dir, s["res_8_vrt"])))
            or (s["res_8_ovr"] and not os.path.isfile(os.path.join(project_dir, s["res_8_ovr"])))
            or (s["complete_vrt"] is None or not os.path.isfile(os.path.join(project_dir, s["complete_vrt"])))
            or (s["complete_ovr"] is None or not os.path.isfile(os.path.join(project_dir, s["complete_ovr"])))
        ):
            missing_subregion_count += 1
            cursor.execute(
                """UPDATE vrt_subregion
                           SET res_2_vrt = ?, res_2_ovr = ?, res_4_vrt = ?,
                           res_4_ovr = ?, res_8_vrt = ?, res_8_ovr = ?,
                           complete_vrt = ?, complete_ovr = ?, built = 0
                           WHERE region = ?""",
                (
                    None,
                    None,
                    None,
                    None,
                    None,
                    None,
                    None,
                    None,
                    s["region"],
                ),
            )
            cursor.execute(
                """UPDATE vrt_utm
                              SET utm_vrt = ?, utm_ovr = ?, built = 0
                              WHERE utm = ?""",
                (
                    None,
                    None,
                    s["utm"],
                ),
            )
            conn.commit()
    return missing_subregion_count


def missing_utms_pmn(project_dir: str, conn: sqlite3.Connection) -> int:
    """
    Confirm built utm's associated VRT and OVR files exists.
    If the files do not exist, then change the utm record to unbuilt.

    Parameters
    ----------
    project_dir
        destination directory for project.
    conn : sqlite3.Connection
        database connection object.

    Returns
    -------
    missing_utm_count : int
        count of UTM zones with missing files.
    """
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM vrt_utm WHERE built_subdataset1 = 1 or built_subdataset2 = 1")
    utms = [dict(row) for row in cursor.fetchall()]
    missing_utm_count = 0
    for utm in utms:
        if (utm["utm_subdataset1_vrt"] is None or utm["utm_subdataset1_ovr"] is None 
            or utm["utm_subdataset2_vrt"] is None or utm["utm_subdataset2_ovr"] is None 
            or utm["utm_combined_vrt"] is None
            or os.path.isfile(os.path.join(project_dir, utm["utm_subdataset1_vrt"])) == False 
            or os.path.isfile(os.path.join(project_dir, utm["utm_subdataset1_ovr"])) == False 
            or os.path.isfile(os.path.join(project_dir, utm["utm_subdataset2_vrt"])) == False 
            or os.path.isfile(os.path.join(project_dir, utm["utm_subdataset2_ovr"])) == False 
            or os.path.isfile(os.path.join(project_dir, utm["utm_combined_vrt"])) == False):
            missing_utm_count += 1
            cursor.execute(
                """UPDATE vrt_utm
                              SET 
                              utm_subdataset1_vrt = ?, utm_subdataset1_ovr = ?, 
                              utm_subdataset2_vrt = ?, utm_subdataset2_ovr = ?, 
                              utm_combined_vrt = ?,
                              built_subdataset1 = 0,
                              built_subdataset2 = 0,
                              built_combined = 0
                              WHERE utm = ?""",
                (
                    None,
                    None,
                    None,
                    None,
                    None,
                    utm["utm"],
                ),
            )
            conn.commit()
    return missing_utm_count


def missing_utms(project_dir: str, conn: sqlite3.Connection) -> int:
    """
    Confirm built utm's associated VRT and OVR files exists.
    If the files do not exist, then change the utm record to unbuilt.

    Parameters
    ----------
    project_dir
        destination directory for project.
    conn : sqlite3.Connection
        database connection object.

    Returns
    -------
    missing_utm_count : int
        count of UTM zones with missing files.
    """
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM vrt_utm WHERE built = 1")
    utms = [dict(row) for row in cursor.fetchall()]
    missing_utm_count = 0
    for utm in utms:
        if utm["utm_vrt"] is None or utm["utm_ovr"] is None or os.path.isfile(os.path.join(project_dir, utm["utm_vrt"])) == False or os.path.isfile(os.path.join(project_dir, utm["utm_ovr"])) == False:
            missing_utm_count += 1
            cursor.execute(
                """UPDATE vrt_utm
                              SET utm_vrt = ?, utm_ovr = ?, built = 0
                              WHERE utm = ?""",
                (
                    None,
                    None,
                    utm["utm"],
                ),
            )
            conn.commit()
    return missing_utm_count


def main(project_dir: str, data_source: str = None, relative_to_vrt: bool = True) -> None:
    """
    Build a gdal VRT for all available tiles.
    This VRT is a collection of smaller areas described as VRTs.
    Nominally 2 meter, 4 meter, and 8 meter data are collected with overviews.
    These data are then added to 16 meter data for the subregion.
    The subregions are then collected into a UTM zone VRT where higher level
    overviews are made.

    Parameters
    ----------
    project_dir
        The directory path to use. Will create if it does not currently exist.
        Required argument.
    data_source : str
        The NBS offers various products to different end-users. Some are available publicly.
        Use this argument to identify which product you want. BlueTopo is the default.
    relative_to_vrt : bool
        Use this argument to set paths of referenced files inside the VRT as relative or absolute paths.

    """
    project_dir = os.path.expanduser(project_dir)
    if os.path.isabs(project_dir) is False:
        print("Please use an absolute path for your project folder.")
        if "windows" not in platform.system().lower():
            print("Typically for non windows systems this means starting with '/'")
        sys.exit(1)

    if int(gdal.VersionInfo()) < 3040000:
        raise RuntimeError("Please update GDAL to >=3.4 to run build_vrt. \n" "Some users have encountered issues with " "conda's installation of GDAL 3.4. " "Try more recent versions of GDAL if you also " "encounter issues in your conda environment.")

    if data_source is None or data_source.lower() == "bluetopo":
        data_source = "BlueTopo"

    elif data_source.lower() == "modeling":
        data_source = "Modeling"

    elif data_source.lower() == "bag":
        data_source = "BAG"

    elif data_source.lower() == "s102v21":
        if int(gdal.VersionInfo()) < 3090000:
            raise RuntimeError("Please update GDAL to >=3.8 to run build_vrt for S102V22.")
        data_source = "S102V21"

    elif data_source.lower() == "s102v22":
        if int(gdal.VersionInfo()) < 3090000:
            raise RuntimeError("Please update GDAL to >=3.8 to run build_vrt for S102V22.")
        data_source = "S102V22"

    elif os.path.isdir(data_source):
        files = os.listdir(data_source)
        files = [file for file in files if file.endswith(".gpkg") and "Tile_Scheme" in file]
        files.sort(reverse=True)
        data_source = None
        for file in files:
            ds_basefile = os.path.basename(file)
            data_source = ds_basefile.split("_")[0]
            break
        if data_source is None:
            raise ValueError(f"Please pass in directory which contains a tile scheme file if you're using a local data source.")

    if not os.path.isdir(project_dir):
        raise ValueError(f"Folder path not found: {project_dir}")

    if not os.path.isfile(os.path.join(project_dir, f"{data_source.lower()}_registry.db")):
        raise ValueError(f"SQLite database not found. Confirm correct folder. " "Note: fetch_tiles must be run at least once prior " "to build_vrt")
    
    if not os.path.isdir(os.path.join(project_dir, data_source)):
        raise ValueError(f"Tile downloads folder not found for {data_source}. Confirm correct folder. " "Note: fetch_tiles must be run at least once prior " "to build_vrt")

    start = datetime.datetime.now()
    print(f"[{start.strftime('%Y-%m-%d %H:%M:%S')} {datetime.datetime.now().astimezone().tzname()}] {data_source}: Beginning work in project folder: {project_dir}\n")
    if data_source.lower() in ("bag", "s102v21"):
        conn = connect_to_survey_registry_pmn1(project_dir, data_source)
    elif data_source.lower() in ("s102v22"):
        conn = connect_to_survey_registry_pmn2(project_dir, data_source)
    else:
        conn = connect_to_survey_registry(project_dir, data_source)
    # subregions missing files
    if data_source.lower() in ("s102v22"):
        missing_subregion_count = missing_subregions_pmn(project_dir, conn)
    else:
        missing_subregion_count = missing_subregions(project_dir, conn)

    if missing_subregion_count:
        print(f"{missing_subregion_count} subregion vrts files missing. " "Added to build list.")

    # build subregion vrts
    if data_source.lower() not in ("s102v22"):
        unbuilt_subregions = select_unbuilt_subregions(conn)
        if len(unbuilt_subregions) > 0:
            print(f"Building {len(unbuilt_subregions)} subregion vrt(s). This may " "take minutes or hours depending on the amount of tiles.")
            for ub_sr in unbuilt_subregions:
                if data_source.lower() in ("bag", "s102v21"):
                    sr_tiles = select_tiles_by_subregion_pmn(project_dir, conn, ub_sr["region"])
                else:
                    sr_tiles = select_tiles_by_subregion(project_dir, conn, ub_sr["region"])
                if len(sr_tiles) < 1:
                    continue
                if data_source.lower() in ("bag", "s102v21"):
                    fields = build_sub_vrts_pmn1(ub_sr, sr_tiles, project_dir, data_source, relative_to_vrt)
                else:
                    fields = build_sub_vrts(ub_sr, sr_tiles, project_dir, data_source, relative_to_vrt)
                update_subregion(conn, fields)
        else:
            print("Subregion vrt(s) appear up to date with the most recently " "fetched tiles.")
    else:
        unbuilt_subregions = select_unbuilt_subregions_pmn(conn)
        if len(unbuilt_subregions) > 0:
            print(f"Building {len(unbuilt_subregions)} subregion vrt(s). This may " "take minutes or hours depending on the amount of tiles.")
            for ub_sr in unbuilt_subregions:
                sr_tiles = select_tiles_by_subregion_pmn(project_dir, conn, ub_sr["region"])
                if len(sr_tiles) < 1:
                    continue
                fields = build_sub_vrts_pmn(ub_sr, sr_tiles, project_dir, data_source, relative_to_vrt)
                update_subregion_pmn(conn, fields)
        else:
            print("Subregion vrt(s) appear up to date with the most recently " "fetched tiles.")

    # utms missing files
    if data_source.lower() in ("s102v22"):
        missing_utm_count = missing_utms_pmn(project_dir, conn)
        if missing_utm_count:
            print(f"{missing_utm_count} utm vrts files missing. Added to build list.")
    else:
        missing_utm_count = missing_utms(project_dir, conn)
        if missing_utm_count:
            print(f"{missing_utm_count} utm vrts files missing. Added to build list.")

    # build utm vrts
    if data_source.lower() not in ("s102v22"):   
        unbuilt_utms = select_unbuilt_utms(conn)
        if len(unbuilt_utms) > 0:
            print(f"Building {len(unbuilt_utms)} utm vrt(s). This may take minutes " "or hours depending on the amount of tiles.")
            for ub_utm in unbuilt_utms:
                utm_start = datetime.datetime.now()
                subregions = select_subregions_by_utm(project_dir, conn, ub_utm["utm"])
                vrt_list = [os.path.join(project_dir, subregion["complete_vrt"]) for subregion in subregions]
                if len(vrt_list) < 1:
                    continue
                rel_path = os.path.join(f"{data_source}_VRT", f"{data_source}_Fetched_UTM{ub_utm['utm']}.vrt")
                utm_vrt = os.path.join(project_dir, rel_path)
                print(f"Building utm{ub_utm['utm']}...")
                if data_source.lower() in ("bag", "s102v21"):
                    create_vrt_pmn1(vrt_list, utm_vrt, [32, 64], relative_to_vrt)
                else:
                    create_vrt(vrt_list, utm_vrt, [32, 64], relative_to_vrt)
                    add_vrt_rat(conn, ub_utm["utm"], project_dir, utm_vrt, data_source)
                fields = {"utm_vrt": rel_path, "utm_ovr": None, "utm": ub_utm["utm"]}
                if os.path.isfile(os.path.join(project_dir, rel_path + ".ovr")):
                    fields["utm_ovr"] = rel_path + ".ovr"
                else:
                    raise RuntimeError("Overview failed to create for " f"utm{ub_utm['utm']}. Please try again. " "If error persists, please contact NBS.")
                update_utm(conn, fields)
                print(f"utm{ub_utm['utm']} complete after {datetime.datetime.now() - utm_start}")
        else:
            print("UTM vrt(s) appear up to date with the most recently " f"fetched tiles.\nNote: deleting the {data_source}_VRT folder will " "allow you to recreate from scratch if necessary")
    else:
        unbuilt_utms = select_unbuilt_utms_pmn(conn)
        if len(unbuilt_utms) > 0:
            print(f"Building {len(unbuilt_utms)} utm vrt(s). This may take minutes " "or hours depending on the amount of tiles.")
            for ub_utm in unbuilt_utms:
                utm_start = datetime.datetime.now()
                subregions = select_subregions_by_utm_pmn(project_dir, conn, ub_utm["utm"])
                vrt_subdataset1_list = [os.path.join(project_dir, subregion["complete_subdataset1_vrt"]) for subregion in subregions]
                vrt_subdataset2_list = [os.path.join(project_dir, subregion["complete_subdataset2_vrt"]) for subregion in subregions]
                if len(vrt_subdataset1_list) < 1 or len(vrt_subdataset2_list) < 1:
                    continue

                rel_subdataset1_path = os.path.join(f"{data_source}_VRT", f"{data_source}_Fetched_UTM{ub_utm['utm']}_BathymetryCoverage.vrt")
                utm_subdataset1_vrt = os.path.join(project_dir, rel_subdataset1_path)

                rel_subdataset2_path = os.path.join(f"{data_source}_VRT", f"{data_source}_Fetched_UTM{ub_utm['utm']}_QualityOfSurvey.vrt")
                utm_subdataset2_vrt = os.path.join(project_dir, rel_subdataset2_path)

                rel_combined_path = os.path.join(f"{data_source}_VRT", f"{data_source}_Fetched_UTM{ub_utm['utm']}.vrt")
                utm_combined_vrt = os.path.join(project_dir, rel_combined_path)

                print(f"Building utm{ub_utm['utm']}...")
                create_vrt_pmn(vrt_subdataset1_list, utm_subdataset1_vrt, [32, 64], relative_to_vrt, subdataset = 1)

                create_vrt_pmn(vrt_subdataset2_list, utm_subdataset2_vrt, [32, 64], relative_to_vrt, subdataset = 2)

                fields = {"utm_subdataset1_vrt": rel_subdataset1_path, 
                          "utm_subdataset2_vrt": rel_subdataset2_path, 
                          "utm_subdataset1_ovr": None, 
                          "utm_subdataset2_ovr": None, 
                          "utm_combined_vrt": utm_combined_vrt,
                          "utm": ub_utm["utm"]}

                if os.path.isfile(os.path.join(project_dir, rel_subdataset1_path + ".ovr")):
                    fields["utm_subdataset1_ovr"] = rel_subdataset1_path + ".ovr"
                else:
                    raise RuntimeError("Overview failed to create for " f"utm{ub_utm['utm']}. Please try again. " "If error persists, please contact NBS.")

                if os.path.isfile(os.path.join(project_dir, rel_subdataset2_path + ".ovr")):
                    fields["utm_subdataset2_ovr"] = rel_subdataset2_path + ".ovr"
                else:
                    raise RuntimeError("Overview failed to create for " f"utm{ub_utm['utm']}. Please try again. " "If error persists, please contact NBS.")

                combine_vrts([utm_subdataset1_vrt, utm_subdataset2_vrt], utm_combined_vrt, relative_to_vrt)

                if data_source.lower() not in ("bag", "s102v21"):
                    if data_source.lower() in ('s102v22'):
                        add_vrt_rat_pmn(conn, ub_utm["utm"], project_dir, utm_combined_vrt, data_source)
                    else:
                        add_vrt_rat(conn, ub_utm["utm"], project_dir, utm_combined_vrt, data_source)

                update_utm_pmn(conn, fields)
                print(f"utm{ub_utm['utm']} complete after {datetime.datetime.now() - utm_start}")

        else:
            print("UTM vrt(s) appear up to date with the most recently " f"fetched tiles.\nNote: deleting the {data_source}_VRT folder will " "allow you to recreate from scratch if necessary")
            
    print(f"[{datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')} {datetime.datetime.now().astimezone().tzname()}] {data_source}: Operation complete after {datetime.datetime.now() - start}")