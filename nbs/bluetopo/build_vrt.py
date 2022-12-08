import os, sqlite3, datetime
import shutil
import sys
from typing import Dict, Any, List, Type
import collections
import numpy as np
from osgeo import gdal

gdal.UseExceptions()
gdal.SetConfigOption("COMPRESS_OVERVIEW", "DEFLATE")

# update final build step to work by branch / utm zone through a query of the vrt_tiles geometry

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

def connect_to_survey_registry(bluetopo_path: str) -> sqlite3.Connection:
    """
    Parameters
    ----------
    bluetopo_path
        path to the bluetopo data as downloaded from AWS

    Returns
    -------
        database connection for registering tiles found in the available BlueTopo datasets.
    """
    database_path = os.path.join(bluetopo_path, 'bluetopo_registry.db')
    conn = None
    try:
        conn = sqlite3.connect(database_path)
    except sqlite3.Error as e:
        print(e)
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
            print(e)
    return conn

def get_tile_scheme(registry_connection: sqlite3.Connection) -> str:
    cursor = registry_connection.cursor()
    cursor.row_factory = sqlite3.Row
    cursor.execute("SELECT * FROM tileset ORDER BY downloaded desc LIMIT 1")
    tilescheme = cursor.fetchone()
    if tilescheme is None:
        raise ValueError('No tilescheme retrieved. Please run fetch_tiles.')
    return tilescheme['location']

def build_sub_vrts(subregion: str, mapped_tiles: list, bluetopo_path: str) -> list:
    """
    Parameters
    ----------
    mapped_tiles
        dictionary of subregion tile names as keys which hold a list of paths to create as subregion vrts.
    bluetopo_path
        path to the bluetopo tiles as downloaded from aws

    Returns
    -------
        list of paths to subregion vrt files
    """
    field_set = {
    'region': subregion['region'],
    'res_2_vrt': None,
    'res_2_ovr': None,
    'res_4_vrt': None,
    'res_4_ovr': None,
    'res_8_vrt': None,
    'res_8_ovr': None,
    'complete_vrt': None,
    'complete_ovr': None}
    end_location = os.path.join('vrt_tiles', subregion['region'])
    region_storage = os.path.join(bluetopo_path, end_location)
    try:
        if os.path.isdir(region_storage):
            shutil.rmtree(region_storage)
    except (OSError, PermissionError) as e:
        print(f'failed to remove older vrt files for {subregion["region"]} \nplease close all files and attempt again')
        sys.exit(1)
    if not os.path.exists(region_storage):
        os.makedirs(region_storage)
    resolution_tiles = collections.defaultdict(list)
    for mapped_tile in mapped_tiles:
        resolution_tiles[mapped_tile['resolution']].append(mapped_tile)
    vrt_list = []
    for resolution, r_tiles in resolution_tiles.items():
        print(f'Building {subregion["region"]} band {resolution}...')
        b_tile_locations = [os.path.join(bluetopo_path, btile['geotiff_disk']) for btile in r_tiles]
        vrt_path = os.path.join(region_storage, subregion["region"] + f'_{resolution}.vrt')
        if '2' in resolution:
            build_vrt(b_tile_locations, vrt_path, [2,4])
            vrt_list.append(vrt_path)
            field_set['res_2_vrt'] = os.path.join(end_location, subregion["region"] + f'_{resolution}.vrt')
            if os.path.isfile(os.path.join(bluetopo_path, field_set['res_2_vrt'] + '.ovr')):
                field_set['res_2_ovr'] = os.path.join(end_location, subregion["region"] + f'_{resolution}.vrt.ovr')
        if '4' in resolution:
            build_vrt(b_tile_locations, vrt_path, [4,8])
            vrt_list.append(vrt_path)
            field_set['res_4_vrt'] = os.path.join(end_location, subregion["region"] + f'_{resolution}.vrt')
            if os.path.isfile(os.path.join(bluetopo_path, field_set['res_4_vrt'] + '.ovr')):
                field_set['res_4_ovr'] = os.path.join(end_location, subregion["region"] + f'_{resolution}.vrt.ovr')
        if '8' in resolution:
            build_vrt(b_tile_locations, vrt_path, [8])
            vrt_list.append(vrt_path)
            field_set['res_8_vrt'] = os.path.join(end_location, subregion["region"] + f'_{resolution}.vrt')
            if os.path.isfile(os.path.join(bluetopo_path, field_set['res_8_vrt'] + '.ovr')):
                field_set['res_8_ovr'] = os.path.join(end_location, subregion["region"] + f'_{resolution}.vrt.ovr')
        if '16' in resolution:
            vrt_list.extend(b_tile_locations)
    complete_vrt_path = os.path.join(end_location, subregion["region"] + '_complete.vrt')
    region_rep = os.path.join(bluetopo_path, complete_vrt_path)
    build_vrt(vrt_list, region_rep, [16])
    field_set['complete_vrt'] = complete_vrt_path
    if os.path.isfile(os.path.join(bluetopo_path, complete_vrt_path + '.ovr')):
        field_set['complete_ovr'] = complete_vrt_path + '.ovr'
    return field_set

def build_vrt(file_list: list, vrt_path: str, levels: list) -> None:
    """
    Parameters
    ----------
    file_list
        list of the file paths to include in the vrt
    vrt_path
        output vrt path
    levels
        list of overview levels to be built with the vrt
    Returns
    -------
        None
    """
    try:
        if os.path.isfile(vrt_path):
            os.remove(vrt_path)
        if os.path.isfile(vrt_path + '.ovr'):
            os.remove(vrt_path + '.ovr')
    except (OSError, PermissionError) as e:
        print(f"failed to remove older vrt files for {vrt_path} \nplease close all files and attempt again")
        sys.exit(1)
    vrt_options = gdal.BuildVRTOptions(srcNodata=np.nan, VRTNodata=np.nan, resampleAlg='near', resolution="highest")
    vrt = gdal.BuildVRT(vrt_path, file_list, options=vrt_options)
    band1 = vrt.GetRasterBand(1)
    band1.SetDescription('Elevation')
    band2 = vrt.GetRasterBand(2)
    band2.SetDescription('Uncertainty')
    band3 = vrt.GetRasterBand(3)
    band3.SetDescription('Contributor')
    vrt = None
    vrt = gdal.Open(vrt_path, 0)
    vrt.BuildOverviews("NEAREST", levels)
    vrt = None

def add_vrt_rat(registry_connection: sqlite3.Connection, utm: str, bt_path: str, vrt_path: str) -> None:
    """
    Parameters
    ----------
    bluetopo_path
        path to the bluetopo dataset as downloaded with the structure on AWS.
    region
        relevant region i.e 'PBC19'
    vrt_path
        path to the vrt to which to add the raster attribute table

    Returns
    -------
        None
    """
    cursor = registry_connection.cursor()
    cursor.row_factory = sqlite3.Row
    cursor.execute("SELECT * FROM tiles WHERE utm = ?", (utm,))
    exp_fields = list(expected_fields.keys())
    tiles = [dict(row) for row in cursor.fetchall()]
    surveys = []
    for tile in tiles:
        gtiff = os.path.join(bt_path, tile['geotiff_disk'])
        if os.path.isfile(gtiff) is False:
            continue
        rat_file = os.path.join(bt_path, tile['rat_disk'])
        if os.path.isfile(rat_file) is False:
            continue
        ds = gdal.Open(gtiff)
        contrib = ds.GetRasterBand(3)
        rat_n = contrib.GetDefaultRAT()
        for col in range(rat_n.GetColumnCount()):
            if exp_fields[col] != rat_n.GetNameOfCol(col).lower():
                raise ValueError('Unexpected field order')
        for row in range(rat_n.GetRowCount()):
            exist = False
            for survey in surveys:
                if survey[0] == rat_n.GetValueAsString(row, 0):
                    survey[1] = str(int(survey[1]) + int(rat_n.GetValueAsString(row, 1)))
                    exist = True
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
            raise TypeError('Unknown data type submitted for gdal raster attribute table.')
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

def select_tiles_by_subregion(bluetopo_path: str, registry_connection: sqlite3.Connection, subregion: str) -> list:
    cursor = registry_connection.cursor()
    cursor.row_factory = sqlite3.Row
    cursor.execute("SELECT * FROM tiles WHERE subregion = ?", (subregion,))
    sr_tiles = [dict(row) for row in cursor.fetchall()]
    sr_tiles_with_files = [sr_tile for sr_tile in sr_tiles if sr_tile['geotiff_disk'] and sr_tile['rat_disk']
                           and os.path.isfile(os.path.join(bluetopo_path, sr_tile['geotiff_disk']))
                           and os.path.isfile(os.path.join(bluetopo_path, sr_tile['rat_disk']))]
    if len(sr_tiles) - len(sr_tiles_with_files) != 0:
        print(f'did not find the files for {len(sr_tiles) - len(sr_tiles_with_files)} registered tile(s) in subregion {subregion}.\n'
              f'you may run fetch_tiles to retrieve files or correct the directory path if incorrect.')
    return sr_tiles_with_files

def select_subregions_by_utm(bluetopo_path: str, registry_connection: sqlite3.Connection, utm: str) -> list:
    cursor = registry_connection.cursor()
    cursor.row_factory = sqlite3.Row
    cursor.execute("SELECT * FROM vrt_subregion WHERE utm = ? and built = 1", (utm,))
    utm_subregions = [dict(row) for row in cursor.fetchall()]
    for utm_subregion in utm_subregions:
        if ((utm_subregion['res_2_vrt'] and os.path.isfile(os.path.join(bluetopo_path, utm_subregion['res_2_vrt'])) == False) or
        (utm_subregion['res_2_ovr'] and os.path.isfile(os.path.join(bluetopo_path, utm_subregion['res_2_ovr'])) == False) or
        (utm_subregion['res_4_vrt'] and os.path.isfile(os.path.join(bluetopo_path, utm_subregion['res_4_vrt'])) == False) or
        (utm_subregion['res_4_ovr'] and os.path.isfile(os.path.join(bluetopo_path, utm_subregion['res_4_ovr'])) == False) or
        (utm_subregion['res_8_vrt'] and os.path.isfile(os.path.join(bluetopo_path, utm_subregion['res_8_vrt'])) == False) or
        (utm_subregion['res_8_ovr'] and os.path.isfile(os.path.join(bluetopo_path, utm_subregion['res_8_ovr'])) == False) or
        (utm_subregion['complete_vrt'] is None or os.path.isfile(os.path.join(bluetopo_path, utm_subregion['complete_vrt'])) == False) or
        (utm_subregion['complete_ovr'] is None or os.path.isfile(os.path.join(bluetopo_path, utm_subregion['complete_ovr'])) == False)):
            raise ValueError(f'subregion vrt files missing for {utm_subregion["utm"]}. please rerun.')
    return utm_subregions

def select_unbuilt_subregions(registry_connection: sqlite3.Connection) -> list:
    cursor = registry_connection.cursor()
    cursor.row_factory = sqlite3.Row
    cursor.execute("SELECT * FROM vrt_subregion WHERE built = 0")
    unbuilt_subregions = [dict(row) for row in cursor.fetchall()]
    return unbuilt_subregions

def select_unbuilt_utms(registry_connection: sqlite3.Connection) -> list:
    cursor = registry_connection.cursor()
    cursor.row_factory = sqlite3.Row
    cursor.execute("SELECT * FROM vrt_utm WHERE built = 0")
    unbuilt_utms = [dict(row) for row in cursor.fetchall()]
    return unbuilt_utms

def update_subregion(registry_connection: sqlite3.Connection, field_set: dict) -> None:
    cursor = registry_connection.cursor()
    cursor.execute('''UPDATE vrt_subregion SET res_2_vrt = ?, res_2_ovr = ?, res_4_vrt = ?, res_4_ovr = ?,
                   res_8_vrt = ?, res_8_ovr = ?, complete_vrt = ?, complete_ovr = ?,
                   built = 1 where region = ?''',
                   (field_set['res_2_vrt'], field_set['res_2_ovr'], field_set['res_4_vrt'], field_set['res_4_ovr'], 
                   field_set['res_8_vrt'], field_set['res_8_ovr'], field_set['complete_vrt'], field_set['complete_ovr'],
                   field_set['region']))
    registry_connection.commit()

def update_utm(registry_connection: sqlite3.Connection, field_set: dict) -> None:
    cursor = registry_connection.cursor()
    cursor.execute('UPDATE vrt_utm SET utm_vrt = ?, utm_ovr = ?, built = 1 where utm = ?',
                   (field_set['utm_vrt'], field_set['utm_ovr'], field_set['utm'],))
    registry_connection.commit()

def missing_subregions(bluetopo_path: str, registry_connection: sqlite3.Connection) -> int:
    cursor = registry_connection.cursor()
    cursor.row_factory = sqlite3.Row
    cursor.execute("SELECT * FROM vrt_subregion WHERE built = 1")
    built_subregions = [dict(row) for row in cursor.fetchall()]
    missing_subregion_count = 0
    # todo comparison against tiles table to know resolution vrts exist where they should
    for subregion in built_subregions:
        if (
        (subregion['res_2_vrt'] and os.path.isfile(os.path.join(bluetopo_path, subregion['res_2_vrt'])) == False) or
        (subregion['res_2_ovr'] and os.path.isfile(os.path.join(bluetopo_path, subregion['res_2_ovr'])) == False) or
        (subregion['res_4_vrt'] and os.path.isfile(os.path.join(bluetopo_path, subregion['res_4_vrt'])) == False) or
        (subregion['res_4_ovr'] and os.path.isfile(os.path.join(bluetopo_path, subregion['res_4_ovr'])) == False) or
        (subregion['res_8_vrt'] and os.path.isfile(os.path.join(bluetopo_path, subregion['res_8_vrt'])) == False) or
        (subregion['res_8_ovr'] and os.path.isfile(os.path.join(bluetopo_path, subregion['res_8_ovr'])) == False) or
        (subregion['complete_vrt'] is None or os.path.isfile(os.path.join(bluetopo_path, subregion['complete_vrt'])) == False) or
        (subregion['complete_ovr'] is None or os.path.isfile(os.path.join(bluetopo_path, subregion['complete_ovr'])) == False)):
            missing_subregion_count += 1
            cursor.execute('''UPDATE vrt_subregion SET res_2_vrt = ?, res_2_ovr = ?, res_4_vrt = ?, res_4_ovr = ?,
                           res_8_vrt = ?, res_8_ovr = ?, complete_vrt = ?, complete_ovr = ?, built = 0 where region = ?''',
                           (None, None, None, None, None, None, None, None, subregion['region'],))
            cursor.execute('UPDATE vrt_utm SET utm_vrt = ?, utm_ovr = ?, built = 0 where utm = ?',
                           (None, None, subregion['utm'],))
            registry_connection.commit()
    return missing_subregion_count

def missing_utms(bluetopo_path: str, registry_connection: sqlite3.Connection) -> int:
    cursor = registry_connection.cursor()
    cursor.row_factory = sqlite3.Row
    cursor.execute("SELECT * FROM vrt_utm WHERE built = 1")
    built_utms = [dict(row) for row in cursor.fetchall()]
    missing_utm_count = 0
    for utm in built_utms:
        if (utm['utm_vrt'] is None or utm['utm_ovr'] is None
        or os.path.isfile(os.path.join(bluetopo_path, utm['utm_vrt'])) == False
        or os.path.isfile(os.path.join(bluetopo_path, utm['utm_ovr'])) == False):
            missing_utm_count += 1
            cursor.execute('UPDATE vrt_utm SET utm_vrt = ?, utm_ovr = ?, built = 0 where utm = ?',
                           (None, None, utm['utm'],))
            registry_connection.commit()
    return missing_utm_count

def main(bluetopo_path:str) -> None:
    """
    Build a gdal VRT for all available tiles.  This VRT is a collection of smaller areas described as VRTs.  Nominally
    4 meter data is collected with overviews, and 8 meter data is also collected with an overview.  These data are then
    added to 16 meter data for the region.  The file used for collecting files is the vrt_tiles geopackage found in
    the data directory.

    Parameters
    ----------
    bluetopo_path
        the path to BlueTopo tiles as downloaded by the fetch_tiles script.

    Returns
    -------
    None
    """
    start = datetime.datetime.now()
    if int(gdal.VersionInfo()) < 3040000:
        raise ValueError('Please update gdal to 3.4 to create VRTs')
    print(f'Beginning work on {bluetopo_path}')
    if not os.path.exists(bluetopo_path):
        raise ValueError("Given BlueTopo Path not found")
    if not os.path.exists(os.path.join(bluetopo_path, 'bluetopo_registry.db')):
        raise ValueError("SQLite DB not found. Confirm correct path. Note: fetch_tiles must be at least once prior to build_vrt")
    conn = connect_to_survey_registry(bluetopo_path)
    tilescheme = os.path.join(bluetopo_path, get_tile_scheme(conn))
    if not os.path.isfile(tilescheme):
        raise ValueError('Failed to find tilescheme. Please run fetch_tiles or correct bluetopo path.')
    # subregions missing files
    missing_sr_count = missing_subregions(bluetopo_path, conn)
    if missing_sr_count:
        print(f'{missing_sr_count} subregion vrts files missing. Added to build list.')
    # build subregion vrts
    unbuilt_subregions = select_unbuilt_subregions(conn)
    print(f'Building {len(unbuilt_subregions)} subregion vrt(s)')
    for ub_sr in unbuilt_subregions:
        sr_tiles = select_tiles_by_subregion(bluetopo_path, conn, ub_sr['region'])
        if len(sr_tiles) < 1:
            continue
        field_set = build_sub_vrts(ub_sr, sr_tiles, bluetopo_path)
        update_subregion(conn, field_set)
    # utms missing files
    missing_utm_count = missing_utms(bluetopo_path, conn)
    if missing_utm_count:
        print(f'{missing_utm_count} utm vrts files missing. Added to build list.')
    # build utm vrts
    unbuilt_utms = select_unbuilt_utms(conn)
    print(f'Building {len(unbuilt_utms)} utm vrt(s)')
    for ub_utm in unbuilt_utms:
        utm_subregions = select_subregions_by_utm(bluetopo_path, conn, ub_utm['utm'])
        vrt_list = [os.path.join(bluetopo_path, utm_subregion['complete_vrt']) for utm_subregion in utm_subregions]
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
            raise ValueError(f"overview not created for utm {str(ub_utm['utm'])}")
        update_utm(conn, field_set)
    total_time = datetime.datetime.now() - start
    print(f'Elapsed time: {total_time}')