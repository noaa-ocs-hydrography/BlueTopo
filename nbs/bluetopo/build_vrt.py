import collections
import datetime
import numpy as np
import os
import shutil
import sqlite3
import sys
from osgeo import gdal

gdal.UseExceptions()
gdal.SetConfigOption("COMPRESS_OVERVIEW", "DEFLATE")

expected_fields = \
    dict(value=[int, gdal.GFU_MinMax],
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
         survey_date_end=[str, gdal.GFU_Generic])

def connect_to_survey_registry(root: str) -> sqlite3.Connection:
    """ 
    Create new or connect to existing SQLite database.

    Parameters
    ----------
    root : str
        destination directory for project.

    Returns
    -------
    conn : sqlite3.Connection
        connection to SQLite database.
    """
    database_path = os.path.join(root, "bluetopo_registry.db")
    conn = None
    try:
        conn = sqlite3.connect(database_path)
        conn.row_factory = sqlite3.Row
    except sqlite3.Error as e:
        print('Failed to establish SQLite database.')
        raise e
    if conn is not None:
        try:
            cursor = conn.cursor()
            cursor.executescript("""CREATE TABLE IF NOT EXISTS tileset (
                                     tilescheme text PRIMARY KEY,
                                     location text,
                                     downloaded text);
                                    CREATE TABLE IF NOT EXISTS vrt_subregion (
                                     region text PRIMARY KEY,
                                     utm text,
                                     res_2_vrt text,
                                     res_2_ovr text,
                                     res_4_vrt text,
                                     res_4_ovr text,
                                     res_8_vrt text,
                                     res_8_ovr text,
                                     complete_vrt text,
                                     complete_ovr text,
                                     built integer);
                                    CREATE TABLE IF NOT EXISTS vrt_utm (
                                     utm text PRIMARY KEY,
                                     utm_vrt text,
                                     utm_ovr text,
                                     built integer);
                                    CREATE TABLE IF NOT EXISTS tiles (
                                     tilename text PRIMARY KEY,
                                     geotiff_link text,
                                     rat_link text,
                                     delivered_date text,
                                     resolution text,
                                     utm text,
                                     subregion text,
                                     geotiff_disk text,
                                     rat_disk text);""")
            conn.commit()
        except sqlite3.Error as e:
            print('Failed to create SQLite tables.')
            raise e
    return conn

def build_sub_vrts(subregion: str, subregion_tiles: list, root: str) -> dict:
    """ 
    Build the VRTs of a given subregion.

    Parameters
    ----------
    subregion
        subregion name.
    subregion_tiles
        list of tile records belonging to subregion.
    root
        destination directory for project.

    Returns
    -------
    field_set : dict
        holds name of subregion and the paths of its VRT and OVR files.
    """
    field_set = {
    "region": subregion["region"],
    "res_2_vrt": None,
    "res_2_ovr": None,
    "res_4_vrt": None,
    "res_4_ovr": None,
    "res_8_vrt": None,
    "res_8_ovr": None,
    "complete_vrt": None,
    "complete_ovr": None}
    end_location = os.path.join("vrt_tiles", subregion["region"])
    region_storage = os.path.join(root, end_location)
    try:
        if os.path.isdir(region_storage):
            shutil.rmtree(region_storage)
    except (OSError, PermissionError) as e:
        print(f"""failed to remove older vrt files for {subregion['region']}
                  please close all files and attempt again""")
        sys.exit(1)
    if not os.path.exists(region_storage):
        os.makedirs(region_storage)
    resolution_tiles = collections.defaultdict(list)
    for subregion_tile in subregion_tiles:
        resolution_tiles[subregion_tile["resolution"]].append(subregion_tile)
    vrt_list = []
    for res, r_tiles in resolution_tiles.items():
        print(f"Building {subregion['region']} band {res}...")
        b_tile_locations = [os.path.join(root, btile["geotiff_disk"]) for btile in r_tiles]
        vrt_path = os.path.join(region_storage, subregion["region"] + f"_{res}.vrt")
        if "2" in res:
            build_vrt(b_tile_locations, vrt_path, [2,4])
            vrt_list.append(vrt_path)
            relative_path = os.path.join(end_location, subregion["region"] + f"_{res}.vrt")
            field_set["res_2_vrt"] = relative_path
            if os.path.isfile(os.path.join(root, field_set["res_2_vrt"] + ".ovr")):
                field_set["res_2_ovr"] = relative_path + ".ovr"
        if "4" in res:
            build_vrt(b_tile_locations, vrt_path, [4,8])
            vrt_list.append(vrt_path)
            relative_path = os.path.join(end_location, subregion["region"] + f"_{res}.vrt")
            field_set["res_4_vrt"] = relative_path
            if os.path.isfile(os.path.join(root, field_set["res_4_vrt"] + ".ovr")):
                field_set["res_4_ovr"] = relative_path + ".ovr"
        if "8" in res:
            build_vrt(b_tile_locations, vrt_path, [8])
            vrt_list.append(vrt_path)
            relative_path = os.path.join(end_location, subregion["region"] + f"_{res}.vrt")
            field_set["res_8_vrt"] = relative_path
            if os.path.isfile(os.path.join(root, field_set["res_8_vrt"] + ".ovr")):
                field_set["res_8_ovr"] = relative_path + ".ovr"
        if "16" in res:
            vrt_list.extend(b_tile_locations)
    relative_path = os.path.join(end_location, subregion["region"] + "_complete.vrt")
    region_rep = os.path.join(root, relative_path)
    build_vrt(vrt_list, region_rep, [16])
    field_set["complete_vrt"] = relative_path
    if os.path.isfile(os.path.join(root, field_set["complete_vrt"] + ".ovr")):
        field_set["complete_ovr"] = relative_path + ".ovr"
    return field_set

def build_vrt(files: list, vrt_path: str, levels: list) -> None:
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
    """
    try:
        if os.path.isfile(vrt_path):
            os.remove(vrt_path)
        if os.path.isfile(vrt_path + ".ovr"):
            os.remove(vrt_path + ".ovr")
    except (OSError, PermissionError) as e:
        print(f"""failed to remove older vrt files for {vrt_path} 
                  please close all files and attempt again""")
        sys.exit(1)
    vrt_options = gdal.BuildVRTOptions(srcNodata=np.nan, 
                                       VRTNodata=np.nan, 
                                       resampleAlg="near", 
                                       resolution="highest")
    vrt = gdal.BuildVRT(vrt_path, files, options=vrt_options)
    band1 = vrt.GetRasterBand(1)
    band1.SetDescription("Elevation")
    band2 = vrt.GetRasterBand(2)
    band2.SetDescription("Uncertainty")
    band3 = vrt.GetRasterBand(3)
    band3.SetDescription("Contributor")
    vrt = None
    vrt = gdal.Open(vrt_path, 0)
    vrt.BuildOverviews("NEAREST", levels)
    vrt = None

def add_vrt_rat(conn: sqlite3.Connection, utm: str, root: str, vrt_path: str) -> None:
    """ 
    Create a raster attribute table for the VRT.

    Parameters
    ----------
    conn : sqlite3.Connection
        A database connection object.
    utm : str
        utm zone of the VRT.
    root
        destination directory for project.
    vrt_path
        path to the VRT to which to add the raster attribute table.
    """
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM tiles WHERE utm = ?", (utm,))
    exp_fields = list(expected_fields.keys())
    tiles = [dict(row) for row in cursor.fetchall()]
    surveys = []
    for tile in tiles:
        gtiff = os.path.join(root, tile["geotiff_disk"])
        if os.path.isfile(gtiff) is False:
            continue
        rat_file = os.path.join(root, tile["rat_disk"])
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
                    # GFU_PixelCount usage has support as int dtype in some software 
                    # so avoiding changing it to python float (double)
                    # this is a temporary solution to avoid overflow error which can 
                    # occur with generalization count in vrts of excessively large coverage
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

def select_tiles_by_subregion(root: str, conn: sqlite3.Connection, subregion: str) -> list:
    """ 
    Retrieve all tile records with files in the given subregion.

    Parameters
    ----------
    root
        destination directory for project.
    conn : sqlite3.Connection
        A database connection object.
    subregion : str
        subregion name.

    Returns
    -------
    sr_tiles_with_files : list
        list of tile records.
    """
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM tiles WHERE subregion = ?", (subregion,))
    sr_tiles = [dict(row) for row in cursor.fetchall()]
    sr_tiles_with_files = [sr_tile for sr_tile in sr_tiles 
                           if sr_tile["geotiff_disk"] and sr_tile["rat_disk"]
                           and os.path.isfile(os.path.join(root, sr_tile["geotiff_disk"]))
                           and os.path.isfile(os.path.join(root, sr_tile["rat_disk"]))]
    if len(sr_tiles) - len(sr_tiles_with_files) != 0:
        print(f"Did not find the files for {len(sr_tiles) - len(sr_tiles_with_files)} "
              f"registered tile(s) in subregion {subregion}. "
               "Run fetch_tiles to retrieve files "
               "or correct the directory path if incorrect.")
    return sr_tiles_with_files

def select_subregions_by_utm(root: str, conn: sqlite3.Connection, utm: str) -> list:
    """ 
    Retrieve all subregion records with files in the given UTM.

    Parameters
    ----------
    root
        destination directory for project.
    conn : sqlite3.Connection
        A database connection object.
    utm : str
        UTM zone.

    Returns
    -------
    utm_subregions : list
        list of subregion records.
    """
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM vrt_subregion WHERE utm = ? and built = 1", (utm,))
    utm_subregions = [dict(row) for row in cursor.fetchall()]
    for sr in utm_subregions:
        if ((sr["res_2_vrt"] and not os.path.isfile(os.path.join(root, sr["res_2_vrt"]))) or
        (sr["res_2_ovr"] and not os.path.isfile(os.path.join(root, sr["res_2_ovr"]))) or
        (sr["res_4_vrt"] and not os.path.isfile(os.path.join(root, sr["res_4_vrt"]))) or
        (sr["res_4_ovr"] and not os.path.isfile(os.path.join(root, sr["res_4_ovr"]))) or
        (sr["res_8_vrt"] and not os.path.isfile(os.path.join(root, sr["res_8_vrt"]))) or
        (sr["res_8_ovr"] and not os.path.isfile(os.path.join(root, sr["res_8_ovr"]))) or
        (sr["complete_vrt"] is None or not os.path.isfile(os.path.join(root, sr["complete_vrt"]))) or
        (sr["complete_ovr"] is None or not os.path.isfile(os.path.join(root, sr["complete_ovr"])))):
            raise ValueError(f"Subregion VRT files missing for {sr['utm']}. please rerun.")
    return utm_subregions

def select_unbuilt_subregions(conn: sqlite3.Connection) -> list:
    """ 
    Retrieve all unbuilt subregion records.

    Parameters
    ----------
    conn : sqlite3.Connection
        A database connection object.

    Returns
    -------
    unbuilt_subregions : list
        list of subregion records.
    """
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM vrt_subregion WHERE built = 0")
    unbuilt_subregions = [dict(row) for row in cursor.fetchall()]
    return unbuilt_subregions

def select_unbuilt_utms(conn: sqlite3.Connection) -> list:
    """ 
    Retrieve all unbuilt utm records.

    Parameters
    ----------
    conn : sqlite3.Connection
        A database connection object.

    Returns
    -------
    unbuilt_utms : list
        list of utm records.
    """
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM vrt_utm WHERE built = 0")
    unbuilt_utms = [dict(row) for row in cursor.fetchall()]
    return unbuilt_utms

def update_subregion(conn: sqlite3.Connection, field_set: dict) -> None:
    """ 
    Update subregion records with given path values.

    Parameters
    ----------
    conn : sqlite3.Connection
        A database connection object.
    field_set : dict
        dictionary with the name of the subregion and paths for its associated VRT and OVR files.
    """
    cursor = conn.cursor()
    cursor.execute("""UPDATE vrt_subregion 
                      SET res_2_vrt = ?, res_2_ovr = ?, res_4_vrt = ?, res_4_ovr = ?,
                      res_8_vrt = ?, res_8_ovr = ?, complete_vrt = ?, complete_ovr = ?,
                      built = 1 where region = ?""",
                     (field_set["res_2_vrt"], field_set["res_2_ovr"], 
                      field_set["res_4_vrt"], field_set["res_4_ovr"], 
                      field_set["res_8_vrt"], field_set["res_8_ovr"], 
                      field_set["complete_vrt"], field_set["complete_ovr"],
                      field_set["region"]))
    conn.commit()

def update_utm(conn: sqlite3.Connection, field_set: dict) -> None:
    """ 
    Update utm records with given path values.

    Parameters
    ----------
    conn : sqlite3.Connection
        A database connection object.
    field_set : dict
        dictionary with the name of the UTM zone and paths for its associated VRT and OVR files.
    """
    cursor = conn.cursor()
    cursor.execute("UPDATE vrt_utm SET utm_vrt = ?, utm_ovr = ?, built = 1 where utm = ?",
                   (field_set["utm_vrt"], field_set["utm_ovr"], field_set["utm"],))
    conn.commit()

def missing_subregions(root: str, conn: sqlite3.Connection) -> int:
    """ 
    Confirm built subregions's associated VRT and OVR files exists. If the files do not exist,
    then change the subregion record to unbuilt.

    Parameters
    ----------
    root
        destination directory for project.
    conn : sqlite3.Connection
        A database connection object.

    Returns
    -------
    missing_subregion_count : int
        count of subregions with missing files.
    """
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM vrt_subregion WHERE built = 1")
    built_subregions = [dict(row) for row in cursor.fetchall()]
    missing_subregion_count = 0
    # todo comparison against tiles table to know resolution vrts exist where they should
    for sr in built_subregions:
        if (
        (sr["res_2_vrt"] and not os.path.isfile(os.path.join(root, sr["res_2_vrt"]))) or
        (sr["res_2_ovr"] and not os.path.isfile(os.path.join(root, sr["res_2_ovr"]))) or
        (sr["res_4_vrt"] and not os.path.isfile(os.path.join(root, sr["res_4_vrt"]))) or
        (sr["res_4_ovr"] and not os.path.isfile(os.path.join(root, sr["res_4_ovr"]))) or
        (sr["res_8_vrt"] and not os.path.isfile(os.path.join(root, sr["res_8_vrt"]))) or
        (sr["res_8_ovr"] and not os.path.isfile(os.path.join(root, sr["res_8_ovr"]))) or
        (sr["complete_vrt"] is None or not os.path.isfile(os.path.join(root, sr["complete_vrt"]))) or
        (sr["complete_ovr"] is None or not os.path.isfile(os.path.join(root, sr["complete_ovr"])))):
            missing_subregion_count += 1
            cursor.execute("""UPDATE vrt_subregion 
                           SET res_2_vrt = ?, res_2_ovr = ?, 
                           res_4_vrt = ?, res_4_ovr = ?, res_8_vrt = ?, res_8_ovr = ?, 
                           complete_vrt = ?, complete_ovr = ?, built = 0 
                           WHERE region = ?""",
                           (None, None, None, None, None, None, None, None, sr["region"],))
            cursor.execute("UPDATE vrt_utm SET utm_vrt = ?, utm_ovr = ?, built = 0 where utm = ?",
                           (None, None, sr["utm"],))
            conn.commit()
    return missing_subregion_count

def missing_utms(root: str, conn: sqlite3.Connection) -> int:
    """ 
    Confirm built utm's associated VRT and OVR files exists. If the files do not exist,
    then change the utm record to unbuilt.

    Parameters
    ----------
    root
        destination directory for project.
    conn : sqlite3.Connection
        A database connection object.

    Returns
    -------
    missing_utm_count : int
        count of UTM zones with missing files.
    """
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM vrt_utm WHERE built = 1")
    built_utms = [dict(row) for row in cursor.fetchall()]
    missing_utm_count = 0
    for utm in built_utms:
        if (utm["utm_vrt"] is None or utm["utm_ovr"] is None
        or os.path.isfile(os.path.join(root, utm["utm_vrt"])) == False
        or os.path.isfile(os.path.join(root, utm["utm_ovr"])) == False):
            missing_utm_count += 1
            cursor.execute("UPDATE vrt_utm SET utm_vrt = ?, utm_ovr = ?, built = 0 where utm = ?",
                           (None, None, utm["utm"],))
            conn.commit()
    return missing_utm_count

def main(bluetopo_path:str) -> None:
    """ 
    Build a gdal VRT for all available tiles. 
    This VRT is a collection of smaller areas described as VRTs.  
    Nominally 2 meter, 4 meter, and 8 meter data are collected with overviews. 
    These data are then added to 16 meter data for the subregion.
    The subregions are then collected into a UTM zone VRT where higher level overviews are made.

    Parameters
    ----------
    bluetopo_path
        destination directory for project.
    """
    start = datetime.datetime.now()
    if int(gdal.VersionInfo()) < 3040000:
        raise ValueError("Please update GDAL to >=3.4 to run build_vrt. \n"
                         "Some users have encountered issues with "
                         "conda's installation of GDAL 3.4. "
                         "Try more recent versions of GDAL "
                         "if you also encounter issues in your conda environment.")
    print(f"Beginning work on {bluetopo_path}")
    if not os.path.exists(bluetopo_path):
        raise ValueError("Given BlueTopo Path not found")
    if not os.path.exists(os.path.join(bluetopo_path, 'bluetopo_registry.db')):
        raise ValueError("SQLite DB not found. Confirm correct path. "
                         "Note: fetch_tiles must be run at least once prior to build_vrt")
    conn = connect_to_survey_registry(bluetopo_path)
    # subregions missing files
    missing_sr_count = missing_subregions(bluetopo_path, conn)
    if missing_sr_count:
        print(f"{missing_sr_count} subregion vrts files missing. Added to build list.")
    # build subregion vrts
    unbuilt_subregions = select_unbuilt_subregions(conn)
    if len(unbuilt_subregions) > 0:
        print(f"Building {len(unbuilt_subregions)} subregion vrt(s) "
               "*note: this may take minutes or hours depending on the amount of data.")
        for ub_sr in unbuilt_subregions:
            sr_tiles = select_tiles_by_subregion(bluetopo_path, conn, ub_sr['region'])
            if len(sr_tiles) < 1:
                continue
            field_set = build_sub_vrts(ub_sr, sr_tiles, bluetopo_path)
            update_subregion(conn, field_set)
    else:
        print("Subregion vrt(s) appear up to date with the most recently fetched tiles.")
    # utms missing files
    missing_utm_count = missing_utms(bluetopo_path, conn)
    if missing_utm_count:
        print(f"{missing_utm_count} utm vrts files missing. Added to build list.")
    # build utm vrts
    unbuilt_utms = select_unbuilt_utms(conn)
    if len(unbuilt_utms) > 0:
        print(f"Building {len(unbuilt_utms)} utm vrt(s) "
               "*note: this may take minutes or hours depending on the amount of data.")
        for ub_utm in unbuilt_utms:
            utm_subregions = select_subregions_by_utm(bluetopo_path, conn, ub_utm['utm'])
            vrt_list = [os.path.join(bluetopo_path, utm_subregion['complete_vrt']) 
                        for utm_subregion in utm_subregions]
            if len(vrt_list) < 1:
                continue
            utm_storage_add = os.path.join('vrt_tiles', str(ub_utm['utm']) + '.vrt')
            utm_storage = os.path.join(bluetopo_path, utm_storage_add)
            print(f"Building utm{ub_utm['utm']}...")
            build_vrt(vrt_list, utm_storage, [32,64])
            add_vrt_rat(conn, ub_utm['utm'], bluetopo_path, utm_storage)
            field_set = {'utm_vrt': utm_storage_add, 'utm_ovr': None, 'utm': ub_utm['utm']}
            if os.path.isfile(os.path.join(bluetopo_path, utm_storage_add + '.ovr')):
                field_set['utm_ovr'] = utm_storage_add + '.ovr'
            else:
                raise ValueError(f"Overview failed to create for utm{ub_utm['utm']}. "
                                  "Please try again. If error persists, please contact NBS.")
            update_utm(conn, field_set)
    else:
        print("UTM vrt(s) appear up to date with the most recently fetched tiles.\n"
              "Note: deleting the vrt_tiles folder will allow you to recreate from scratch if necessary")
    total_time = datetime.datetime.now() - start
    print(f"Operation complete. Elapsed time: {total_time}")