import os, glob, sqlite3, datetime
from typing import Dict, Any, List, Type

import numpy as np
from osgeo import gdal, ogr
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

def get_available_tiles(bluetopo_path: str) -> dict:
    """
    Parameters
    ----------
    bluetopo_path
        path to bluetopo data.
    Returns
    -------
    dict of tile names (keys) to tile paths (str value).
    """
    search_path = os.path.join(bluetopo_path, 'BlueTopo/*/*.tiff')
    available_files = glob.glob(search_path)
    available_tiles = {}
    if len(available_files) > 0:
        for file_path in available_files:
            root = os.path.dirname(file_path)
            tile_name = os.path.split(root)[-1]
            available_tiles[tile_name] = file_path
    return available_tiles


def modify_contributors(available_tiles: dict, bluetopo_path: str) -> None:
    """
    Parameters
    ----------
    available_tiles
        a list of the available tiles
    bluetopo_path
        path to the bluetopo data as downloaded from AWS
    Returns
    -------
        None
    """
    registry = connect_to_survey_registry(bluetopo_path)
    try:
        for tile in available_tiles:
            nbs_tile_ds = gdal.Open(available_tiles[tile],1)
            geotransform = nbs_tile_ds.GetGeoTransform()
            xres = geotransform[1]
            contributor_band = nbs_tile_ds.GetRasterBand(3)
            no_data = int(contributor_band.GetNoDataValue())
            rat = contributor_band.GetDefaultRAT()
            # check the table to see if it is as expected
            for idx, field_name in enumerate(expected_fields):
                rat_field = rat.GetNameOfCol(idx).lower()
                if rat_field == 'value':
                    survey_idx_col = idx
                if rat_field == 'source_survey_id':
                    survey_id_col = idx
                if rat_field != field_name:
                    raise ValueError(f'RAT columns do not match expected columns: {rat_field} vs {field_name}')
            # extract the rat values
            update_rat = False
            rat_vals = []
            for row in range(rat.GetRowCount()):
                row_vals = []
                for col in range(rat.GetColumnCount()):
                    row_vals.append(rat.GetValueAsString(row, col))
                old_survey_idx = int(row_vals[survey_idx_col])
                new_survey_info = get_survey_idx(row_vals[survey_id_col], registry)
                if new_survey_info is None:
                    new_survey_info = add_survey_idx(tuple(row_vals), registry)
                if old_survey_idx != new_survey_info[survey_idx_col]:
                    update_rat = True
                rat_vals.append(new_survey_info)
            if not update_rat:
                # hey, it looks like this file doesn't need to be updated...
                continue
            else:
                # okay, fine.  Update the contributor layer
                print(f'Updating {tile} RAT')
                old_contrib_band_array = contributor_band.ReadAsArray()
                # confirm all values in the array are in the table?
                new_contrib_band_array = np.full_like(old_contrib_band_array, no_data)
                new_rat = rat.Clone()
                for idx, row in enumerate(rat_vals):
                    old_survey_idx = rat.GetValueAsInt(idx, survey_idx_col)
                    contrib_idx = np.where(old_contrib_band_array == old_survey_idx)
                    new_contrib_band_array[contrib_idx] = row[survey_idx_col]
                    new_rat.SetValueAsInt(idx, survey_idx_col, row[survey_idx_col])
                contributor_band.WriteArray(new_contrib_band_array)
                contributor_band.SetDefaultRAT(new_rat)
            if xres == 4.0:
                nbs_tile_ds.BuildOverviews('Nearest', [2,4])
            elif xres == 8.0:
                nbs_tile_ds.BuildOverviews('Nearest', [2])
    except Exception as e:
        raise e
    finally:
        registry.close()



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
    database_path = os.path.join(bluetopo_path, 'survey_registry.db')
    conn = None
    try:
        conn = sqlite3.connect(database_path)
    except sqlite3.Error as e:
        print(e)
    if conn is not None:
        try:
            sql_create_survey_registry_table = """ CREATE TABLE IF NOT EXISTS survey_registry (
                                                        value integer PRIMARY KEY,
                                                        count float,
                                                        data_assessment integer,
                                                        feature_least_depth integer,
                                                        significant_features integer,
                                                        feature_size float,
                                                        coverage integer,
                                                        bathy_coverage integer,
                                                        horizontal_uncert_fixed float,
                                                        horizontal_uncert_var float,
                                                        vertical_uncert_fixed float,
                                                        vertical_uncert_var float,
                                                        license_name text,
                                                        license_url text,
                                                        source_survey_id text NOT NULL,
                                                        source_institution text,
                                                        survey_date_start text,
                                                        survey_date_end text
                                                        
                                                ); """
            curser = conn.cursor()
            curser.execute(sql_create_survey_registry_table)
        except sqlite3.Error as e:
            print(e)
    return conn


def get_survey_idx(survey_id: str, registry_connection: sqlite3.Connection) -> tuple:
    """
    Get the survey information from the sqlite database.

    Parameters
    ----------
    survey_id
        survey name as a string as pulled from the raster attribute table field source_survey_id.
    registry_connection
        pysqlite database connection object.
    Returns
    -------
        a tuple with the information belonging to the survey_id from the database.  Returns None if the survey_id is
        not found.
    """
    cursor = registry_connection.cursor()
    cursor.execute("SELECT * FROM survey_registry WHERE source_survey_id=?", (survey_id,))
    surveys = cursor.fetchall()
    num_surveys = len(surveys)
    if num_surveys > 1:
        raise ValueError(f'Duplicate Survey IDs found in registry: {surveys}')
    elif num_surveys == 1:
        survey_info = surveys[0]
    else:
        survey_info = None
    return survey_info


def add_survey_idx(survey_info: tuple, registry_connection: sqlite3.Connection) -> tuple:
    """
    Add the given survey information to the sqlite database.

    Parameters
    ----------
    survey_info
        A tuple of the
    registry_connection
        A database connection object

    Returns
    -------
        A tuple of survey info from the database once included and containing the new RAT index value
    """
    expected_field_names = expected_fields.keys()
    idx = list(expected_field_names).index('source_survey_id')
    survey_id = survey_info[idx]
    if get_survey_idx(survey_id, registry_connection) is None:
        cursor = registry_connection.cursor()
        cursor.execute('''INSERT INTO survey_registry(count, data_assessment, feature_least_depth,
                                                        significant_features, feature_size, coverage, bathy_coverage,
                                                        horizontal_uncert_fixed, horizontal_uncert_var, 
                                                        vertical_uncert_fixed, vertical_uncert_var, license_name, 
                                                        license_url, source_survey_id, source_institution, 
                                                        survey_date_start, survey_date_end) 
                                                        VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)''', survey_info[1:])
        registry_connection.commit()
    survey_info = get_survey_idx(survey_id, registry_connection)
    return survey_info


def group_available_tiles(available_tiles: dict, bluetopo_path: str, vrt_tile_path: str) -> dict:
    """
    Group the available bluetopo tiles by intersecting the bluetopo geometries with the vrt tile path geometries.

    Parameters
    ----------
    available_tiles
        A dictionary of the available tiles by name and the associated path to the tile raster file
    bluetopo_path
        path to the bluetopo tiles as downloaded from AWS
    vrt_tile_path
        path to the tesselation scheme to use for grouping tiles

    Returns
    -------
        A dictionary of vrt subregion names pointing to a list of bluetopo tiles belonging to each tile.
    """
    # get the bluetopo tiles
    bluetopo_tesselation_pattern = os.path.join(bluetopo_path,'BlueTopo', 'BlueTopo-Tile-Scheme','BlueTopo_Tile_Scheme*.gpkg')
    bluetopo_tesselation = glob.glob(bluetopo_tesselation_pattern)
    if len(bluetopo_tesselation) == 0:
        raise ValueError(f'No BlueTopo tesselation found matching {bluetopo_tesselation_pattern}')
    elif len(bluetopo_tesselation) > 1:
        raise ValueError(f'More than one BlueTopo tesselation found matching {bluetopo_tesselation_pattern}')
    else:
        bluetopo_tesselation = bluetopo_tesselation[0]
    bluetopo_ds = ogr.Open(bluetopo_tesselation)
    bluetopo_lyr = bluetopo_ds.GetLayer(0)
    lyr_name = bluetopo_lyr.GetName()
    # get geometries for the available tiles
    sql = f'SELECT * FROM {lyr_name} WHERE tile IN {tuple(available_tiles.keys())}'
    available_tiles_lyr = bluetopo_ds.ExecuteSQL(sql)
    # build intersection
    target = ogr.Open(vrt_tile_path)
    grouped_filename = os.path.join(bluetopo_path, 'intersection_file.gpkg')
    driver = ogr.GetDriverByName('GPKG')
    intersection = driver.CreateDataSource(grouped_filename)
    intersect_lyr = intersection.CreateLayer('intersect_lyr', geom_type=ogr.wkbPolygon)
    target_layer = target.GetLayer(0)
    target_layer.Intersection(available_tiles_lyr, intersect_lyr)
    # build the tile map
    lyr_def = intersect_lyr.GetLayerDefn()
    for field_num in range(lyr_def.GetFieldCount()):
        field = lyr_def.GetFieldDefn(field_num)
        if field.name == 'tile':
            tileid = field_num
        elif field.name == 'CellName':
            cellid = field_num
    tile_map = {}
    feature = intersect_lyr.GetNextFeature()
    while feature:
        cell = feature.GetField(cellid)
        tile = feature.GetField(tileid)
        band = tile[2]
        if cell not in tile_map:
            tile_map[cell] = {}
            geometry = feature.GetGeometryRef()
            centroid = geometry.Centroid()
            utm_zone = int(1 + (180 + centroid.GetX()) / 6)
            tile_map[cell]['region'] = feature['Branch'] + str(utm_zone).zfill(2)
        if band not in tile_map[cell]:
            tile_map[cell][band] = []
        tile_map[cell][band].append(available_tiles[tile])
        feature = intersect_lyr.GetNextFeature()
    region_map = {}
    for cell_name in tile_map:
        region = tile_map[cell_name]['region']
        if region not in region_map:
            region_map[region] = {}
        region_map[region][cell_name] = tile_map[cell_name]
    intersection = None
    os.remove(grouped_filename)
    return region_map


def build_sub_vrts(mapped_tiles: dict, bluetopo_path: str) -> list:
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
    storage_dir = os.path.join(bluetopo_path,'vrt_tiles')
    if not os.path.exists(storage_dir):
        os.makedirs(storage_dir)
    vrt_list = []
    total_regions = len(mapped_tiles)
    for num, region in enumerate(mapped_tiles):
        print(f'Building subregion {region} ({num + 1}/{total_regions}).')
        region_storage = os.path.join(storage_dir,region)
        if not os.path.exists(region_storage):
            os.makedirs(region_storage)
        region_files = []
        if '5' in mapped_tiles[region]:
            band5_vrt = os.path.join(region_storage, 'band5.vrt')
            build_vrt(mapped_tiles[region]['5'], band5_vrt, [4,8])
            region_files.append(band5_vrt)
        if '4' in mapped_tiles[region]:
            band4_vrt = os.path.join(region_storage, 'band4.vrt')
            build_vrt(mapped_tiles[region]['4'], band4_vrt, [8])
            region_files.append(band4_vrt)
        if '3' in mapped_tiles[region]:
            region_files.append(mapped_tiles[region]['3'][0])
        if len(region_files) == 1:
            region_rep = region_files[0]
        else:
            region_rep = os.path.join(storage_dir, region + '.vrt')
            build_vrt(region_files, region_rep, [16])
        vrt_list.append(region_rep)
    return vrt_list

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
    vrt_options = gdal.BuildVRTOptions(srcNodata=1000000, VRTNodata=1000000, resolution="highest")
    vrt = gdal.BuildVRT(vrt_path, file_list, options=vrt_options)
    band1 = vrt.GetRasterBand(1)
    band1.SetDescription('Elevation')
    band2 = vrt.GetRasterBand(2)
    band2.SetDescription('Uncertainty')
    band3 = vrt.GetRasterBand(3)
    band3.SetDescription('Contributor')
    vrt = None
    vrt = gdal.Open(vrt_path, 0)
    vrt.BuildOverviews("nearest", levels)
    vrt = None


def add_vrt_rat(bluetopo_path: str, vrt_path: str) -> None:
    """
    Parameters
    ----------
    bluetopo_path
        path to the bluetopo dataset as downloaded with the structure on AWS.
    vrt_path
        path to the vrt to which to add the raster attribute table

    Returns
    -------
        None
    """
    # build a gdal rat table by adding columns of the right type
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
    # add the registry to the rat
    registry = connect_to_survey_registry(bluetopo_path)
    cursor = registry.cursor()
    cursor.execute("SELECT * FROM survey_registry")
    surveys = cursor.fetchall()
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
    # assign the table to the vrt
    vrt_ds = gdal.Open(vrt_path, 1)
    contributor_band = vrt_ds.GetRasterBand(3)
    contributor_band.SetDefaultRAT(rat)
    vrt_ds = None


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
    vrt_tile_path = os.path.join(os.path.dirname(__file__), 'data/vrt_tiles.gpkg')
    print(f'Beginning work on {bluetopo_path}')
    start = datetime.datetime.now()
    available_tiles = get_available_tiles(bluetopo_path)
    modify_contributors(available_tiles, bluetopo_path)
    regionally_mapped_tiles = group_available_tiles(available_tiles, bluetopo_path, vrt_tile_path)
    for region in regionally_mapped_tiles:
        mapped_tiles = regionally_mapped_tiles[region]
        print(f'Found {len(mapped_tiles)} subtiles in {region} to create from {len(available_tiles)} BlueTopo tiles.')
        vrt_list = build_sub_vrts(mapped_tiles, bluetopo_path)
        vrt_name = f'_{region}.'.join([bluetopo_path,'vrt'])
        build_vrt(vrt_list, vrt_name, [32,64])
        add_vrt_rat(bluetopo_path, vrt_name)
    total_time = datetime.datetime.now() - start
    print(f'Elapsed time: {total_time}')