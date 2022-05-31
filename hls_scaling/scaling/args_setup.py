import os
import argparse
import warnings

from scaling import utility


def parse_args():
    parser = argparse.ArgumentParser(
                description='DSWx-HLS Scaling Script. Script queries STAC-CMR database, filters the results based on given parameters, downloads the matching HLS tiles, and processes them through DSWx-HLS.',
                formatter_class=argparse.ArgumentDefaultsHelpFormatter
                )

    # parser.add_argument('--params_file',
    #                     dest='params_file',
    #                     type=str,
    #                     default='',
    #                     help='The use of --params_file input will ignore all other command line arguments. This argument takes the path to a Python module file (e.g. params.py) that contains an "args dict".'
    #                     )

    parser.add_argument('--root_dir','--root',
                        dest='root_dir',
                        type=str,
                        default='.',
                        help='Path to directory to store the request results.'
                        )

    parser.add_argument('--job_name','--name',
                        dest='job_name',
                        type=str,
                        default='StudyArea',
                        help='Name of the request'
                        )

    msg = '''
    Area to observe. Format is W Longitude,S Latitude,E Longitude,N Latitude. 
    Coordinates are enclosed in quotes and separated by a space.
    Ex: "-120 43 -118 48"
    '''
    parser.add_argument('--bbox', '--bounding_box', '--bb',
                        dest='bounding_box',
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
    parser.add_argument('--date_range',
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
    'The maximum percentage of the image with cloud cover.
    Must be an integer in range [0,100]. Default is 100.'
    0 == no clouds present, 100 == images could be fully covered in clouds
    '''
    parser.add_argument('--cloud_cover_max','--cc',
                        dest='cloud_cover_max',
                        type=int,
                        default=100,
                        help=msg
                        )

    msg = '''
    The minimum percentage of the tile with data.
    Must be an integer in range [0,100]. Default is 0.'
    0 == image can have no pixels with satellite data, 100 == all pixels have satellite data
    '''
    parser.add_argument('--spatial_coverage_min', '--sc',
                        dest='spatial_coverage_min',
                        type=int,
                        default=0,
                        help=msg
                        )

    msg = '''
    Flag to filter for granules where L30 and S30 images exist for the same tile(s) 
    on the same date. Note that these "same day" occurances are relatively rare.
    '''
    parser.add_argument('--same_day',
                        dest='same_day',
                        action='store_true',
                        default=False,
                        help=msg
                        )

    msg = '''
    Flag to skip downloading and processing the tiles through DSWx-HLS PROTEUS.
    This will still save a record of the filtered query results <root_dir>/<job_name>.
    To later download and process these results, do two things:
    1) Edit the file <root_dir>/<job_name>/settings.json
    so that do_not_download is False and do_not_process is False.
    2) Use the --rerun flag, e.g. ```python hls_scaling_script.py --root_dir <root_dir> --name <job_name> --rerun```
    '''
    parser.add_argument('--do_not_download',
                        dest='do_not_download',
                        action='store_true',
                        default=False,
                        help=msg
                        )

    msg = '''
    Flag to skip processing the tiles through DSWx-HLS PROTEUS.
    This will still save a record of the filtered query results <root_dir>/<job_name>,
    and it will download the HLS tiles into the correct input directory structure.
    To later process these results, do two things:
    1) Edit the file <root_dir>/<job_name>/settings.json
    so that do_not_process is False.
    2) Use the --rerun flag, e.g. ```python hls_scaling_script.py --root_dir <root_dir> --name <job_name> --rerun```
    '''
    parser.add_argument('--do_not_process',
                        dest='do_not_process',
                        action='store_true',
                        default=False,
                        help=msg
                        )

    msg = '''Flag to rerun an existing Study Area. If included, then all CL arguments
        will be ignored, except root_dir and job_name (which are mandatory).
        If selected, script will look in the directory <root_dir>/<job_name> for 
        these files: 'settings.json' and 'query_results.pickle'.
        (These files should have been auto-generated by the scaling script.)
        Script will download any invalid or un-downloaded HLS tiles. It will then 
        do a new processing of all tiles from the original Study Area query.
        Ex: python hls_scaling_script.py –root_dir . --name StudyAreaTest --rerun
        '''

    parser.add_argument('--rerun',
                        dest='rerun',
                        action='store_true',
                        default=False,
                        help=msg
                        )

    msg = '''
    Template PROTEUS Runconfig file. In the default template, 
    all values surrounded by $$<VALUE>$$ will be
    replaced with the appropriate paths by the scaling script.
    To alter other items (such as the output DSWx-HLS products),
    please edit those fields in the template file before running the scaling script.
    '''
    parser.add_argument('--runconfig_template',
                        dest='runconfig_template',
                        type=str,
                        default='./scaling/dswx_hls_runconfig_template.txt',
                        help=msg
                        )

    msg = '''
    World-wide dem.vrt file. This dem is used for all tiles processed.
    Note: in the future, if a world-wide dem file is no longer available
    and individual dem files must be fetched for each tile, then the
    workflow for populating each granule's runconfig file will need to be updated.
    See: desired_granules.py > create_runconfig_yaml()
    '''
    parser.add_argument('--dem_file',
                        dest='dem_file',
                        type=str,
                        default='/home/shiroma/dat/nisar-dem-copernicus/EPSG4326.vrt',
                        help=msg
                        )

    msg = '''
    Bands to download for L30 (Landsat) HLS 2.0 files.
    Bands must be separated by commas and have no spaces.
    For PROTEUS, the following L30 bands are required: 
    B02, B03, B04, B05, B06, B07, Fmask.
    Defaults to: 'B02,B03,B04,B05,B06,B07,Fmask'
    '''
    parser.add_argument('--l30_v2_bands',
                    dest='l30_v2_bands',
                    type=str,
                    default='B02,B03,B04,B05,B06,B07,Fmask',
                    help=msg
                    )

    msg = '''
    Bands to download for S30 (Sentinel) HLS 2.0 files.
    Bands must be separated by commas and have no spaces.
    For PROTEUS, the following L30 bands are required: 
    B02, B03, B04, B8A, B11, B12, Fmask.
    Defaults to: 'B02,B03,B04,B8A,B11,B12,Fmask'
    '''
    parser.add_argument('--s30_v2_bands',
                    dest='s30_v2_bands',
                    type=str,
                    default='B02,B03,B04,B8A,B11,B12,Fmask',
                    help=msg
                    )

    msg = '''
    STAC Server to query.
    For HLS, use LPCLOUD: https://cmr.earthdata.nasa.gov/stac/LPCLOUD/
    Defaults to: 'https://cmr.earthdata.nasa.gov/stac/LPCLOUD/'
    '''
    parser.add_argument('--STAC_URL_LPCLOUD',
                    dest='STAC_URL_LPCLOUD',
                    type=str,
                    default='https://cmr.earthdata.nasa.gov/stac/LPCLOUD/',
                    help=msg
                    )

    msg = '''
    Collections to query.
    Separate collection names by a comma.
    HLS v2.0 L30 and S30 use these collections: HLSL30.v2.0 and HLSS30.v2.0, respectively.
    Defaults to: 'HLSL30.v2.0,HLSS30.v2.0'
    '''
    parser.add_argument('--COLLECTIONS',
                    dest='COLLECTIONS',
                    type=str,
                    default='HLSL30.v2.0,HLSS30.v2.0',
                    help=msg
                    )

    # parser.add_argument('-v', '--verbose',
    #                     dest='verbose',
    #                     action='store_true',
    #                     default=False,
    #                     help='Display progress, warnings, and errors in Terminal'
    #                     )

    # Put args into a dictionary for ease of access
    args = vars(parser.parse_args())

    # # If params_file file was provided, replace the parsed args dict with the args dict from the params file.
    # if not args['rerun']:
    #     if os.path.exists(args['params_file']):
    #         import importlib
    #         params = importlib.import_module(name='..params', package='scaling.params')

    #         print(len(args))

    #         for arg in args.keys():
    #             assert arg in params, \
    #                 print("params_file %s is missing the input argument %s" % (args['params_file'], arg))

    #         args = params.args

    #     # params_file defaults to the empty string.
    #     # So, if a params_file was provided but the file does not exist, raise an error.
    #     elif args['params_file']:
    #         raise ImportError('--params_file argument provided but the given file does not exist: ', args['params_file'])

    return args


def verify_args(args, raw_input=True):
    '''
    This function does a basic check that the raw input arguments are correctly formatted.
    Note: bounding_box and date_range will not be verified here. Those are direct input
    arguments to pystac-client, which will handle their verification.

    raw_input : (bool) True if inputs are parsed directly from the command line, etc. and are unprocessed.
                        False if the inputs have already been adjusted for use, such as in prepare_args().
    '''
    assert os.path.isdir(args['root_dir']), f"{args['root_dir']} is not a valid directory."
    assert isinstance(args['job_name'], str), f"{args['job_name']} must be a string."
    assert isinstance(args['date_range'], str), "A date_range must be provided."
    assert 0 <= args['cloud_cover_max'] and args['cloud_cover_max'] <= 100, \
        "cloud_cover_max input must be between 0 and 100, inclusive."
    assert 0 <= args['spatial_coverage_min'] and args['spatial_coverage_min'] <= 100, \
        "spatial_coverage_min input must be between 0 and 100, inclusive."
    assert isinstance(args['same_day'], bool), "same_day input must be Boolean."
    assert isinstance(args['do_not_download'], bool), "do_not_download input must be Boolean."
    assert isinstance(args['do_not_process'], bool), "do_not_process input must be Boolean."
    assert isinstance(args['rerun'], bool), "rerun input must be Boolean."

    if args['rerun']:
        study_area_dir = os.path.join(args['root_dir'], args['job_name'])
        assert os.path.isdir(study_area_dir), "For --rerun option, directory %s must exist." % study_area_dir
        assert os.path.exists(os.path.join(study_area_dir, 'settings.json')), \
            "%s is missing and required for --rerun. If error persists, please remove --rerun option to begin scaling script from scratch." % os.path.join(study_area_dir, 'settings.json')
        assert os.path.exists(os.path.join(study_area_dir, 'query_results.pickle')), \
            "%s is missing and required for --rerun. If error persists, please remove --rerun option to begin scaling script from scratch." % os.path.join(study_area_dir, 'query_results.pickle')

    if raw_input:
        if not args['rerun']:
            assert isinstance(args['bounding_box'], str), "A bounding_box must be provided."

        assert set(args['months'].split(',')).issubset(["Jan","Feb","Mar","Apr","May","Jun","Jul","Aug","Sep","Oct","Nov","Dec"]), \
            "months input must be a subset of 'Jan,Feb,Mar,Apr,May,Jun,Jul,Aug,Sep,Oct,Nov,Dec'."

        assert set(args['l30_v2_bands'].split(',')).issubset(['B09','VZA','SAA','B10','B03','B05','Fmask','B07','B02','SZA','B04','B06','B01','VAA','B11','browse','metadata']), \
            "months input must be a subset of 'B09,VZA,SAA,B10,B03,B05,Fmask,B07,B02,SZA,B04,B06,B01,VAA,B11,browse,metadata'."
        if not set(['B02', 'B03', 'B04', 'B05', 'B06', 'B07', 'Fmask']).issubset(args['l30_v2_bands'].split(',')):
            warnings.warn("The requested l30_v2_bands are missing bands needed to run PROTEUS for L30 granules.")

        assert set(args['s30_v2_bands'].split(',')).issubset(['B06','SZA','B07','B10','B09','B05','B08','B02','VAA','B01','SAA','B8A','B04','B12','Fmask','B11','B03','VZA','browse','metadata']), \
            "months input must be a subset of 'B06,SZA,B07,B10,B09,B05,B08,B02,VAA,B01,SAA,B8A,B04,B12,Fmask,B11,B03,VZA,browse,metadata''."
        if not set(['B02', 'B03', 'B04', 'B8A', 'B11', 'B12', 'Fmask']).issubset(args['s30_v2_bands'].split(',')):
            warnings.warn("The requested s30_v2_bands are missing bands needed to run PROTEUS for S30 granules.")

        assert set(args['COLLECTIONS'].split(',')).issubset(['HLSL30.v2.0','HLSS30.v2.0']), \
            print('Only HLSL30.v2.0 and/or HLSS30.v2.0 currently supported for --COLLECTIONS input.')

    # else input has already been "prepped" for use by the scaling script
    else:
        if not args['rerun']:
            assert all(isinstance(args['bounding_box'], (int, float))), "A bounding_box must have only int or float data types."
            assert all(isinstance(args['months'], int)), "The months should have been converted to their numeric form. See utility/month_to_num()"

        assert set(args['l30_v2_bands']).issubset(['B09','VZA','SAA','B10','B03','B05','Fmask','B07','B02','SZA','B04','B06','B01','VAA','B11','browse','metadata']), \
            "months input must be a subset of 'B09,VZA,SAA,B10,B03,B05,Fmask,B07,B02,SZA,B04,B06,B01,VAA,B11,browse,metadata'."
        if not set(['B02', 'B03', 'B04', 'B05', 'B06', 'B07', 'Fmask']).issubset(args['l30_v2_bands']):
            warnings.warn("The requested l30_v2_bands are missing bands needed to run PROTEUS for L30 granules.")

        assert set(args['s30_v2_bands']).issubset(['B06','SZA','B07','B10','B09','B05','B08','B02','VAA','B01','SAA','B8A','B04','B12','Fmask','B11','B03','VZA','browse','metadata']), \
            "months input must be a subset of 'B06,SZA,B07,B10,B09,B05,B08,B02,VAA,B01,SAA,B8A,B04,B12,Fmask,B11,B03,VZA,browse,metadata''."
        if not set(['B02', 'B03', 'B04', 'B8A', 'B11', 'B12', 'Fmask']).issubset(args['s30_v2_bands']):
            warnings.warn("The requested s30_v2_bands are missing bands needed to run PROTEUS for S30 granules.")

        assert set(args['COLLECTIONS']).issubset(['HLSL30.v2.0','HLSS30.v2.0']), \
            print('Only HLSL30.v2.0 and/or HLSS30.v2.0 currently supported for --COLLECTIONS input.')


def prepare_args(args):
    '''
    This function transforms the parsed input arguments into the
    format needed for hls_scaling_script.py.
    '''
    # Transform bounding_box from string into a list of numbers.
    # Ex: '-120 43 -118 48'  -->  [-120, 43, -118, 48]
    if isinstance(args['bounding_box'], str):
        args['bounding_box'] = [float(i) for i in args['bounding_box'].split()]

    # Transform months from string into a list of numbers.
    # Ex: 'Jun,Jul,Aug'  -->  [6, 7, 8]
    if isinstance(args['months'], str):
        args['months'] = args['months'].split(',')
        args['months'] = [utility.month_to_num(month) for month in args['months']]

    # Transform l30 bands from string into a list of strings.
    # Ex: 'B02,B03,B04,B05,B06,B07,Fmask'  -->  ['B02', 'B03', 'B04', 'B05', 'B06', 'B07', 'Fmask']
    if isinstance(args['l30_v2_bands'], str):
        args['l30_v2_bands'] = args['l30_v2_bands'].split(',')

    # Transform s30 bands from string into a list of strings.
    # Ex: 'B02,B03,B04,B8A,B11,B12,Fmask'  -->  ['B02', 'B03', 'B04', 'B8A', 'B11', 'B12', 'Fmask']
    if isinstance(args['s30_v2_bands'], str):
        args['s30_v2_bands'] = args['s30_v2_bands'].split(',')

    # Transform COLLECTIONS from string into a list of strings.
    # Ex: 'HLSL30.v2.0,HLSS30.v2.0' -->  ['HLSL30.v2.0', 'HLSS30.v2.0']
    if isinstance(args['COLLECTIONS'], str):
        args['COLLECTIONS'] = args['COLLECTIONS'].split(',')
