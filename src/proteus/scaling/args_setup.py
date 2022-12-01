import os
import argparse
import warnings
import json
import re
import copy

from proteus.scaling import utility


def parse_args():
    parser = argparse.ArgumentParser(
                description='DSWx-HLS Scaling Script. Script queries STAC-CMR database, filters the results based on given parameters, downloads the matching HLS granules, and processes them through DSWx-HLS.',
                formatter_class=argparse.ArgumentDefaultsHelpFormatter
                )

    msg = '''
    Path to a runconfig .json file that contains values for all possible Command Line inputs.
    Use the same input formats as normal command-line inputs; see --help for details.
    (See scaling/scaling_runconfig_example.json for an example.)
    NOTE: The values in this runconfig file will supercede all other command line inputs.
    '''
    parser.add_argument('--scaling-runconfig',
                        dest='scaling_runconfig',
                        type=str,
                        default='',
                        help=msg
                        )

    parser.add_argument('--root-dir','--root',
                        dest='root_dir',
                        type=str,
                        default='.',
                        help='Path to directory to store the request results.'
                        )

    parser.add_argument('--job-name','--name',
                        dest='job_name',
                        type=str,
                        default='StudyArea',
                        help='Name of the request'
                        )

    ncpu = os.cpu_count()
    msg = '''
    Max number of CPUs to use for downloading/processing the granules.
    Should be less than or equal to the number of available CPUs.
    Defaults to the number of CPUs available on the system.
    '''
    parser.add_argument('--ncpu',
                        dest='ncpu',
                        type=int,
                        default=ncpu,
                        help=msg
                        )

    msg = '''
    Area to observe. Format is W Longitude,S Latitude,E Longitude,N Latitude. 
    Coordinates are enclosed in quotes and separated by a space.
    Ex: "-120 43 -118 48"
    '''
    parser.add_argument('--bbox', '--bounding-box', '--bb',
                        dest='bounding_box',
                        type=str,
                        default='',                        
                        help=msg
                        )

    msg = '''
    Path to a .geojson file of the area to observe. Format of Geojson
    is per the STAC API Spec. These formats include:
    GeoJSON Point (object) or GeoJSON LineString (object) or 
    GeoJSON Polygon (object) or GeoJSON MultiPoint (object) or 
    GeoJSON MultiLineString (object) or GeoJSON MultiPolygon (object) (GeoJSON Geometry) 
    '''
    parser.add_argument('--intersects',
                        dest='intersects',
                        type=str,
                        default='',                        
                        help=msg
                        )

    msg = '''
    HLS 2.0 granule ID(s) to download and process. If multiple granule IDs,
    they should be separated by a comma. If this argument is provided,
    all other query filters will be ignored.
    Ex: 'HLS.L30.T04WFT.2022076T214936.v2.0' or 
    'HLS.L30.T04WFT.2022076T214936.v2.0,HLS.S30.T04WFT.2022076T220529.v2.0'
    '''
    parser.add_argument('--granule-ids',
                        dest='granule_ids',
                        type=str,
                        default='',                        
                        help=msg
                        )

    msg = '''
    String representing the date range to search.
    Either a single datetime or datetime range can be used.
    Examples: "2020-06-02/2021-06-30", "2021-08-13/2021-08", "2020"
    For details on allowable dates, see the 'datetime' parameter
    in: https://pystac-client.readthedocs.io/en/latest/api.html#stac-api-io
    (This scaling script uses pystac-client's API to query STAC, so the input 
    format is the same.)
    '''
    parser.add_argument('--date-range',
                        dest='date_range',
                        type=str,
                        default='',
                        help=msg
                        )

    msg = '''
    String of Select Months within the date_range to search.
    This should be a subset of "Jan,Feb,Mar,Apr,May,Jun,Jul,Aug,Sep,Oct,Nov,Dec".
    Example input: "Jun,Jul,Aug"
    Use the first three letters of the desired months, and separate with a comma.
    Note: if none of these months fall within the date_range, there will be zero search results.
    '''
    parser.add_argument('--months','--mo',
                        dest='months',
                        type=str,
                        default="Jan,Feb,Mar,Apr,May,Jun,Jul,Aug,Sep,Oct,Nov,Dec",
                        help=msg
                        )

    msg = '''
    MGRS Tile ID to download and process. Only one tile ID is supported.
    Providing a Tile ID will ignore --bounding_box and --intersects arguments.
    Ok to prepend a 'T' to the tile ID; it should match the pattern
    'NNLLL' or 'TNNLLL', e.g. '16WFT' or 'T16WFT'.
    '''
    parser.add_argument('--tile-id',
                        dest='tile_id',
                        type=str,
                        default='',                        
                        help=msg
                        )

    msg = '''
    'The maximum percentage of a granule's image with cloud cover.
    Must be an integer in range [0,100].'
    0 == no clouds present, 100 == images could be fully covered in clouds
    '''
    parser.add_argument('--cloud-cover-max','--cc',
                        dest='cloud_cover_max',
                        type=int,
                        default=100,
                        help=msg
                        )

    msg = '''
    The minimum percentage of a granule's tile that must have data.
    Must be an integer in range [0,100].'
    0 == image can have no pixels with satellite data, 100 == all pixels have satellite data
    '''
    parser.add_argument('--spatial-coverage-min', '--sc',
                        dest='spatial_coverage_min',
                        type=int,
                        default=0,
                        help=msg
                        )

    msg = '''
    Flag to filter for granules where L30 and S30 granules exist for the same tile(s) 
    on the same date. Note that these "same day" occurances are relatively rare.
    '''
    parser.add_argument('--same-day',
                        dest='same_day',
                        action='store_true',
                        default=False,
                        help=msg
                        )

    msg = '''
    Flag to skip downloading and processing the granules through DSWx-HLS PROTEUS.
    This will still save a record of the filtered query results <root_dir>/<job_name>.
    To later download and process these results, do two things:
    1) Edit the file <root_dir>/<job_name>/settings.json
    so that do_not_download is False and do_not_process is False.
    2) Use the --rerun flag, e.g. ```python hls_scaling_script.py --root_dir <root_dir> --name <job_name> --rerun```
    '''
    parser.add_argument('--do-not-download',
                        dest='do_not_download',
                        action='store_true',
                        default=False,
                        help=msg
                        )

    msg = '''
    Flag to skip processing the granules through DSWx-HLS PROTEUS.
    This will still save a record of the filtered query results <root_dir>/<job_name>,
    and it will download the HLS granules into the correct input directory structure.
    To later process these results, do two things:
    1) Edit the file <root_dir>/<job_name>/settings.json
    so that do_not_process is False.
    2) Use the --rerun flag, e.g. ```python hls_scaling_script.py --root-dir <root-dir> --name <job-name> --rerun```
    '''
    parser.add_argument('--do-not-process',
                        dest='do_not_process',
                        action='store_true',
                        default=False,
                        help=msg
                        )

    msg = '''Flag to "rerun" an existing Study Area.
        If selected, script will look in the directory <root_dir>/<job_name> for 
        these files: 'settings.json' and 'query_results.pickle'. (These files 
        should have been auto-generated by the scaling script during the original
        time it was run.)
        Any input parameters for querying and filtering the STAC server will be ignored.
        Script will download any invalid or un-downloaded HLS granules, and it will then 
        (re)process all granules from the original Study Area query. To prevent downloading
        or processing, use the --do_not_download or --do_not_process flags.
        Example usage: python hls_scaling_script.py -–root_dir . --name StudyAreaTest --rerun
        WARNING: --rerun will not re-run the query + filtering step. To update
        these parameters, please create a new job.
        '''

    parser.add_argument('--rerun',
                        dest='rerun',
                        action='store_true',
                        default=False,
                        help=msg
                        )

    msg = '''
    Path to default runconfig .yaml file for dswx_hls.py.
    The scaling script will keep all settings in the .yaml as-is, 
    with the exceptions of: input_dir, dem_file, landcover_file, 
    worldcover_file, scratch_dir, output_dir, product_path, and product_id
    which the scaling script will automatically customize for 
    the desired study area.
    '''
    parser.add_argument('--runconfig-yaml',
                        dest='runconfig_yaml',
                        type=str,
                        default='./src/proteus/defaults/dswx_hls.yaml',
                        help=msg
                        )

    msg = '''
    World-wide dem.vrt file. This dem is used for all tiles processed.
    Warning: in the future, if a world-wide dem file is no longer available
    and individual dem files must be fetched for each tile, then the
    workflow for populating each granule's runconfig file will need to be updated.
    See: download_and_process.py > create_runconfig_yaml()
    '''
    parser.add_argument('--dem-file',
                        dest='dem_file',
                        type=str,
                        default='/home/shiroma/dat/nisar-dem-copernicus/EPSG4326.vrt',
                        help=msg
                        )

    msg = '''
    World-wide landcover file. This landcover file is used for all tiles processed.
    Warning: in the future, if a world-wide landcover file is no longer available
    and an individual landcover file must be fetched for each tile, then the
    workflow for populating each granule's runconfig file will need to be updated.
    See: download_and_process.py > create_runconfig_yaml()
    '''
    parser.add_argument('--landcover-file',
                        dest='landcover_file',
                        type=str,
                        default='/home/shiroma/dat/copernicus_landcover/PROBAV_LC100_global_v3.0.1_2019-nrt_Discrete-Classification-map_EPSG-4326.tif',
                        help=msg
                        )

    msg = '''
    World-wide worldcover file. This worldcover file is used for all tiles processed.
    Warning: in the future, if a world-wide worldcover file is no longer available
    and an individual worldcover file must be fetched for each tile, then the
    workflow for populating each granule's runconfig file will need to be updated.
    See: download_and_process.py > create_runconfig_yaml()
    '''
    parser.add_argument('--worldcover-file',
                        dest='worldcover_file',
                        type=str,
                        default='/mnt/aurora-r0/jungkyo/data/landcover.vrt',
                        help=msg
                        )

    msg = '''
    Bands to download for L30 (Landsat) HLS 2.0 files.
    Bands must be separated by commas and have no spaces.
    For PROTEUS, the following L30 bands are required: 
    B02, B03, B04, B05, B06, B07, Fmask.
    '''
    parser.add_argument('--l30-v2-bands',
                    dest='l30_v2_bands',
                    type=str,
                    default='B02,B03,B04,B05,B06,B07,Fmask,browse',
                    help=msg
                    )

    msg = '''
    Bands to download for S30 (Sentinel) HLS 2.0 files.
    Bands must be separated by commas and have no spaces.
    For PROTEUS, the following L30 bands are required: 
    B02, B03, B04, B8A, B11, B12, Fmask.
    '''
    parser.add_argument('--s30-v2-bands',
                    dest='s30_v2_bands',
                    type=str,
                    default='B02,B03,B04,B8A,B11,B12,Fmask,browse',
                    help=msg
                    )

    msg = '''
    STAC Server to query.
    For HLS, use LPCLOUD: https://cmr.earthdata.nasa.gov/stac/LPCLOUD/
    '''
    parser.add_argument('--stac-url-lpcloud',
                    dest='stac_url_lpcloud',
                    type=str,
                    default='https://cmr.earthdata.nasa.gov/stac/LPCLOUD/',
                    help=msg
                    )

    msg = '''
    Collections to query.
    Separate collection names by a comma.
    HLS v2.0 L30 and S30 use these collections: HLSL30.v2.0 and HLSS30.v2.0, respectively.
    '''
    parser.add_argument('--collections',
                    dest='collections',
                    type=str,
                    default='HLSL30.v2.0,HLSS30.v2.0',
                    help=msg
                    )

    parser.add_argument('-v', '--verbose',
                        dest='verbose',
                        action='store_true',
                        default=False,
                        help='Display additional progress, warnings, and error indicators in console window.'
                        )

    # Put command line args into a dictionary for ease of access
    args = vars(parser.parse_args())

    # If scaling_runconfig file was provided, replace the parsed args with the 
    # contents of the scaling_runconfig file.
    if os.path.exists(args['scaling_runconfig']):
        with open(args['scaling_runconfig'], 'r') as f:
            args = json.load(f)

    # scaling_runconfig defaults to the empty string.
    # So, if a scaling_runconfig was provided but the file does not exist, raise an error.
    elif args['scaling_runconfig']:
        raise ImportError('--scaling_runconfig file was provided but the given file does not exist: ', args['scaling_runconfig'])

    # If --rerun is selected, then replace all args with the previous settings
    # This must come after parsing args['scaling_runconfig'], in case that file
    # specifies the rerun option.
    if args['rerun']:
        # Place input parameters into temporary variables
        tmp_root_dir = args['root_dir']
        tmp_job_name = args['job_name']
        tmp_do_not_download = args['do_not_download']
        tmp_do_not_process = args['do_not_process']
        tmp_verbose = args['verbose']

        job_dir = os.path.join(args['root_dir'], args['job_name'])
        assert os.path.isdir(job_dir), f"For --rerun, {job_dir} must already be a valid directory."

        # Load prior settings from saved settings.json file into the args.
        # This will replace all other args inputs.
        settings_json = os.path.join(job_dir, 'settings.json')

        with open(settings_json, 'r') as f:
            args = json.load(f)

        # Make sure 'rerun' is still set to True
        args['rerun'] = True

        # Update settings parsed from the .json with the user's new requests:
        args['root_dir'] = tmp_root_dir
        args['job_name'] = tmp_job_name
        args['do_not_download'] = tmp_do_not_download
        args['do_not_process'] = tmp_do_not_process
        args['verbose'] = tmp_verbose

    return args


def verify_input_args(args):
    '''
    This function does a basic check that the raw input arguments are correctly formatted.
    Note: the values of bounding_box and date_range will not be verified here.
    Those are direct input arguments to pystac-client; pystac-client will handle their verification.

    '''
    # Job setup inputs
    assert isinstance(args['rerun'], bool), "rerun input must be Boolean."
    assert isinstance(args['do_not_download'], bool), "do_not_download input must be Boolean."
    assert isinstance(args['do_not_process'], bool), "do_not_process input must be Boolean."
    assert os.path.isdir(args['root_dir']), f"{args['root_dir']} is not a valid directory."
    assert args['job_name'], "--job_name must be provided and not an empty string."
    assert args['ncpu'] > 0 and isinstance(args['ncpu'], int), "--ncpu must be greater than 0 and an integer"
    if args['ncpu'] > os.cpu_count():
        warnings.warn(f"The provided --ncpu was {args['ncpu']}, but there are only {os.cpu_count()} available CPUs. Reducing ncpu to {os.cpu_count()}.")
        args['ncpu'] = os.cpu_count()

    # Inputs needed for processing in PROTEUS' DSWx HLS
    assert os.path.exists(args['dem_file']), f"--dem_file was input as (or defaulted to) {args['dem_file']}, but that file does not exist."
    assert os.path.exists(args['landcover_file']), f"--landcover_file was input as (or defaulted to) {args['landcover_file']}, but that file does not exist."
    assert os.path.exists(args['worldcover_file']), f"--worldcover_file was input as (or defaulted to) {args['worldcover_file']}, but that file does not exist."

    # Inputs needed for downloading specific bands for the HLS granules
    assert set(args['l30_v2_bands'].split(',')).issubset(['B09','VZA','SAA','B10','B03','B05','Fmask','B07','B02','SZA','B04','B06','B01','VAA','B11','browse','metadata']), \
        "months input must be a subset of 'B09,VZA,SAA,B10,B03,B05,Fmask,B07,B02,SZA,B04,B06,B01,VAA,B11,browse,metadata'."
    if not set(['B02', 'B03', 'B04', 'B05', 'B06', 'B07', 'Fmask']).issubset(args['l30_v2_bands'].split(',')):
        warnings.warn("The requested l30_v2_bands are missing bands needed to run PROTEUS for L30 granules.")

    assert set(args['s30_v2_bands'].split(',')).issubset(['B06','SZA','B07','B10','B09','B05','B08','B02','VAA','B01','SAA','B8A','B04','B12','Fmask','B11','B03','VZA','browse','metadata']), \
        "months input must be a subset of 'B06,SZA,B07,B10,B09,B05,B08,B02,VAA,B01,SAA,B8A,B04,B12,Fmask,B11,B03,VZA,browse,metadata''."
    if not set(['B02', 'B03', 'B04', 'B8A', 'B11', 'B12', 'Fmask']).issubset(args['s30_v2_bands'].split(',')):
        warnings.warn("The requested s30_v2_bands are missing bands needed to run PROTEUS for S30 granules.")

    # For --rerun, check that the required files exist.
    if args['rerun']:
        study_area_dir = os.path.join(args['root_dir'], args['job_name'])
        assert os.path.isdir(study_area_dir), "For --rerun option, directory %s must already exist." % study_area_dir
        assert os.path.exists(os.path.join(study_area_dir, 'settings.json')), \
            "%s is missing and required for --rerun. If error persists, please remove --rerun option to begin scaling script from scratch." % os.path.join(study_area_dir, 'settings.json')
        assert os.path.exists(os.path.join(study_area_dir, 'query_results.pickle')), \
            "%s is missing and required for --rerun. If error persists, please remove --rerun option to begin scaling script from scratch." % os.path.join(study_area_dir, 'query_results.pickle')

    # Granule ids were provided, so we can skip verifying the query filter inputs
    if args['granule_ids']:
        # Each granule ID must match the pattern of HLS 2.0 naming convention,
        # e.g. HLS.L30.T04WFT.2022076T214936.v2.0
        pattern = re.compile(r'^HLS\.[LS]30\.T\d{2}[A-Z]{3}\.20\d{5}T\d{6}\.v2\.0$')
        for gran in args['granule_ids'].split(','):
            assert pattern.match(gran), "For --granule-id, the provided granule ID %s does not match HLS 2.0 specification." % gran

    # A new Study Area job is requested; check the inputs used for filtering
    else:
        assert args['date_range'], "A date_range must be provided and not an empty string."

        # make sure tile-id matches the provided pattern
        if args['tile_id']:
            # The tile ID must match the pattern of MGRS tiles. Ok if it has a 'T' at the beginning.,
            # e.g. '04WFT' or 'T04WFT' are ok
            pattern1 = re.compile(r'\d{2}[A-Z]{3}')
            pattern2 = re.compile(r'T\d{2}[A-Z]{3}')
            assert pattern1.match(args['tile_id']) or pattern2.match(args['tile_id']), \
                f"For --tile-id, the provided MGRS Tile ID {tile} does not match the pattern 'NNLLL' nor 'TNNLLL'."
        else:
            # Must have provide either a bounding box, intersects input, or tile-id, but not more than one
            msg = "Either a bounding box, tile id, or intersects argument must be provided, but not more than one."
            assert bool(args['bounding_box']) ^ bool(args['intersects']) ^ bool(args['tile_id']), msg
            if args['intersects']:
                assert os.path.exists(args['intersects']), "--intersects argument should be a path to a GEOJSON file."

        assert 0 <= args['cloud_cover_max'] and args['cloud_cover_max'] <= 100, \
            "cloud_cover_max input must be between 0 and 100, inclusive."
        assert 0 <= args['spatial_coverage_min'] and args['spatial_coverage_min'] <= 100, \
            "spatial_coverage_min input must be between 0 and 100, inclusive."
        assert isinstance(args['same_day'], bool), "same_day input must be Boolean."

        assert set(args['months'].split(',')).issubset(["Jan","Feb","Mar","Apr","May","Jun","Jul","Aug","Sep","Oct","Nov","Dec"]), \
            "months input must be a subset of 'Jan,Feb,Mar,Apr,May,Jun,Jul,Aug,Sep,Oct,Nov,Dec'."

        assert set(args['collections'].split(',')).issubset(['HLSL30.v2.0','HLSS30.v2.0']), \
            'Only HLSL30.v2.0 and/or HLSS30.v2.0 currently supported for --collections input.'



def reformat_args(args):
    '''
    This function transforms the parsed input arguments into the
    format needed for hls_scaling_script.py.
    '''
    args_clean = copy.deepcopy(args)

    # If a tile ID was provided, get its bounding box.
    if args['tile_id'] and isinstance(args['tile_id'], str):
        base_tile_id = args['tile_id'].upper().lstrip('T')
        bbox = utility.mgrs_to_lat_lon_bounding_box(base_tile_id)

        # Save Bounding Box to args
        args_clean['bounding_box'] = list(bbox)

    # If bounding_box is a non-empty string, transform it from string into a list of numbers.
    # Ex: '-120 43 -118 48'  -->  [-120, 43, -118, 48]
    if args['bounding_box'] and isinstance(args['bounding_box'], str):
        args_clean['bounding_box'] = [float(i) for i in args['bounding_box'].split()]

    # Transform months from string into a list of numbers.
    # Ex: 'Jun,Jul,Aug'  -->  [6, 7, 8]
    if isinstance(args['months'], str):
        args_clean['months'] = args['months'].split(',')
        args_clean['months'] = [utility.month_to_num(month) for month in args_clean['months']]

    # Transform l30 bands from string into a list of strings.
    # Ex: 'B02,B03,B04,B05,B06,B07,Fmask'  -->  ['B02', 'B03', 'B04', 'B05', 'B06', 'B07', 'Fmask']
    if isinstance(args['l30_v2_bands'], str):
        args_clean['l30_v2_bands'] = args['l30_v2_bands'].split(',')

    # Transform s30 bands from string into a list of strings.
    # Ex: 'B02,B03,B04,B8A,B11,B12,Fmask'  -->  ['B02', 'B03', 'B04', 'B8A', 'B11', 'B12', 'Fmask']
    if isinstance(args['s30_v2_bands'], str):
        args_clean['s30_v2_bands'] = args['s30_v2_bands'].split(',')

    # Transform collections from string into a list of strings.
    # Ex: 'HLSL30.v2.0,HLSS30.v2.0' -->  ['HLSL30.v2.0', 'HLSS30.v2.0']
    if isinstance(args['collections'], str):
        args_clean['collections'] = args['collections'].split(',')

    # If granule IDs were provided, transform them from string into a list of strings.
    # Ex: 'HLS.L30.T04WFT.2022076T214936.v2.0,HLS.S30.T04WFT.2022076T220529.v2.0'
    #       --> ['HLS.L30.T04WFT.2022076T214936.v2.0', 'HLS.S30.T04WFT.2022076T220529.v2.0']
    if args['granule_ids'] and isinstance(args['granule_ids'], str):
        args_clean['granule_ids'] = args['granule_ids'].split(',')

    return args_clean
