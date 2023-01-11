import os
import sys
import requests
import warnings
import subprocess
from multiprocessing.pool import ThreadPool
from itertools import repeat

import xml.etree.ElementTree as ET
from ruamel.yaml import YAML
from osgeo import gdal

from proteus.scaling import utility
from proteus import dswx_hls

'''
Notes on the implementation strategy:

By using ThreadPool, .starmap() will send each granule
to its own thread to run download_and_process_granule().
ThreadPool also handles the limiting of the number of 
threads (and later processes) created, so as not to overwhelm
the system with potentially thousands of requests at once.

Internally, download_and_process_granule() first downloads (in serial)
all of the necessary .tif files for that granule, and 
second it generates a dswx_hls.py runconfig.yaml file and
spawns a subprocess to process that granule in dswx_hls.py.

Threads are well-suited for downloading files, and limiting the
number of concurrent downloads to the number of threads should
help with considerations for bandwidth and mitigating
HTTP 429 "Too Many Requests" errors from LPDAAC. However, this is
(educated) rationale, and there could be room for improvement.

subprocess.run() will spawn a new process in the OS for each granule
to process it through dswx_hls.py.
Like any process, the OS will handle scheduling these either 
in parallel or concurrently based on available CPU resources.
A benefit of using subprocess.run() with a runconfig.yaml file
is that the runconfig script can be stored and reviewed at a 
later date.

Possible design changes:
* To call dswx_hls.py natively in Python and skip using the runconfig.yaml
file, two items need refactoring:
    1) process_granule()'s call to subprocess.run would need to be
    changed to the generate_dswx_layers(...)
    2) "from multiprocessing.pool import ThreadPool" should become
    "from multiprocessing import Pool", and then 
    "pool = ThreadPool(os.cpu_count()) should become "pool = Pool(os.cpu_count())"
    This will cause each call to download_and_process_granule() to be
    spawned into a new process for the OS to manange. Overall run time will
    be similar (perhaps a few seconds longer), and we lose 
    the reproducibility of the runconfig.yaml.
* By having both the downloading and processing in the same function,
we remove latency between the time .tif files are downloaded and
when their processing can begin. Alternatively, to separate downloading
and processing into two separate calls would either create a barrier
between them (latency costs), or would require a Queue'ing system
to remove that delay, which adds complexity.
'''

def download_and_process_granules(job_dir, query_results_dict, args):

    # Get list of granule IDs.
    list_of_granule_ids = list(query_results_dict.keys())

    # Create directory structure for all granules.
    # Save this list to a txt file.
    # Note: Do this before the pool operation to avoid race conditions
    # (e.g. duplicated directories).
    list_of_granule_dirs = []
    with open(os.path.join(job_dir,'granule_dir.txt'), 'a+') as f:
        for granule_id in query_results_dict.keys():
            sensor, tileID, date = utility.get_sensor_tileID_date(granule_id)
            granule_dir = create_dir_structure(job_dir,[date,tileID,sensor])

            ### TEMP FOR lower48 same day request!
            # granule_dir = create_dir_structure(job_dir,[sensor])
            ### end tmp code

            list_of_granule_dirs.append(granule_dir)

            f.write("%s\n" % granule_dir)

        # remove final "\n" newline character
        utility.remove_last_char(f)

    # For each granule, get list of URLs to download
    # Get the list of urls to download for this Granule
    all_lists_of_urls = \
        [utility.get_list_of_urls(query_results_dict, granule_id, args['l30_v2_bands'], args['s30_v2_bands']) \
            for granule_id in query_results_dict.keys()]

    # Limit the maximum number of simultaneous downloads and processes to
    # the number of available CPUs.
    print(f"Starting a ThreadPool of {args['ncpu']} threads...")
    pool = ThreadPool(args['ncpu'])

    # Downloaded and process each granule independently
    if args['do_not_process']:
        print("Beginning downloading of granules. Per the --do-not-process input, these will not be processed in dswx_hls.py.")
    else:
        print("Beginning downloading and processing of granules in dswx_hls.py.")

    results = pool.starmap(download_and_process_granule, \
                    zip(list_of_granule_ids, \
                        list_of_granule_dirs, \
                        all_lists_of_urls, \
                        repeat(args)))

    if args['do_not_process']:
        print(f"Downloading of granules complete. Location: {job_dir}")
        if not all(results):
            warnings.warn("Some of the granules were not successfully downloaded.")

    else:
        print(f"Downloading and processing of granules complete. Location: {job_dir}")
        if not all(results):
            warnings.warn("Some of the granules were not successfully downloaded and/or processed.")


def create_dir_structure(job_dir=".", list_of_subdirs=None):
    """
    Inputs:
    -------
    job_dir : (str) path to an existing parent directory; this is where
            the new subdirectories will be rooted
            Ex: job_dir = '~/dev/HLSScalingJob'

    list_of_subdirs : (list of strings) a list of ordered strings,
                    where each entry is will be a subdirectory of the item before it.
            Ex: list_of_subdirs = ['20200624','T11TLH','S30']

    Side Effects:
    -------------
    This function ensures that each directory along the 
    output cur_path either exists or it makes that directory.
    In the last directory in list_of_subdirs, this function creates 
    'input_dir','output_dir', and 'scratch_dir' directories.

    Returns:
    --------
    full_path : (str) path to desired directory

    """
    cur_path = job_dir

    for subdir in list_of_subdirs:
        cur_path = os.path.join(cur_path, subdir)
        if not os.path.isdir(cur_path):
            os.makedirs(cur_path)

    for dir in ['input_dir', 'output_dir', 'scratch_dir']:
        tmp_dir = os.path.join(cur_path,dir)
        if not os.path.isdir(tmp_dir):
            os.makedirs(tmp_dir)

    return cur_path


def download_and_process_granule(granule_id, dir_path, list_of_urls, args):

    # Download granule data
    try:
        download_granule_data(granule_id, list_of_urls, dir_path, args)

    except Exception as e:
        print(e)
        warnings.warn("An Exception occurred. Granule %s could not be downloaded and will not be processed." % granule_id)
        return False

    # Process granule through DSWx-HLS (dswx_hls.py)
    if not args['do_not_process']:
        # Create runconfig file
        runconfig_path = create_runconfig_yaml(dir_path, granule_id, list_of_urls, args)

        # Create path to the dswx-hls.py log file
        log_path = os.path.join(dir_path,'dswx_hls_log.txt')

        # Remove any leftover files from output_dir and scratch_dir
        for f in os.listdir(os.path.join(dir_path, 'output_dir')):
            os.remove(os.path.join(os.path.join(dir_path, 'output_dir'), f))
        for f in os.listdir(os.path.join(dir_path, 'scratch_dir')):
            os.remove(os.path.join(os.path.join(dir_path, 'scratch_dir'), f))

        # Process through DSWx-HLS (dswx_hls.py)
        (proteus_stdout, proteus_stderr) = process_granule(granule_id, runconfig_path, log_path, args)

        # TODO: If there are errors from dswx_hls.py, standard practise says that
        # this function should return False.
        # However, there are several warnings occuring in dswx_hls.py that are being
        # sent to stderr, but the file outputs from dswx-hls.py look "ok",
        # so ignore those warnings/errors for now.

    return True


def download_granule_data(granule_id, list_of_urls, dir_path, args):
    if args['verbose']:
        print('Beginning Download of granule ID %s...' % granule_id)

    for url in list_of_urls:
        # NOTE: os.path.basename() does not handle edge cases for urls.
        # But, the urls from the STAC queries are standardized, so
        # use this until more robustness is needed.
        file_name = os.path.join(dir_path, 'input_dir', os.path.basename(url))

        # If file does not exist, then download it
        if not os.path.exists(file_name):
            utility.download2file(url, file_name)
        
        # if file exists but is not a valid COG, then re-download it
        elif not utility.valid_GeoTiff(file_name):
            os.remove(file_name)
            utility.download2file(url, file_name)
        
        # else: File already exists and is valid. Do not re-download.

    if args['verbose']:
        print('Download of granule ID %s complete.' % granule_id)


def process_granule(granule_id, runconfig_path, log_path, args):

    if args['verbose']:
        print('Beginning processing of granule ID %s...' % granule_id)

    # Process through PROTEUS
    # Note that PROTEUS must be alread installed on the system to run.
    # See: https://github.com/opera-adt/PROTEUS for instructions.
    p = subprocess.run(['dswx_hls.py', runconfig_path, '--log-file', log_path], \
                        capture_output=True, text=True
                        )
                        
    # If process resulted in an error, then do something
    if p.stderr:
        warnings.warn("Warnings/Errors were generated from dswx_hls.py while processing granule ID %s. To display these errors to console, re-process the granule via this scaling script by using the --rerun AND --verbose flags." % granule_id)
        if args['verbose']:
            print(p.stderr)

    elif args['verbose']:
        print('Processing in dswx_hls.py of granule ID %s complete.' % granule_id)

    return (p.stdout, p.stderr)


def create_runconfig_yaml(granule_dir_path, granule_id, list_of_urls, args):

    # Load the default dswx_hls.yaml template
    yaml = YAML()
    with open(args['runconfig_yaml'], 'r') as rc:
        runconfig = yaml.load(rc)

    # Update the yaml with the desired arguments
    runconfig['runconfig']['groups']['input_file_group']['input_file_path'] = \
                [os.path.join(granule_dir_path, 'input_dir')]

    runconfig['runconfig']['groups']['dynamic_ancillary_file_group']['dem_file'] = \
                os.path.join(args['dem_file'])

    runconfig['runconfig']['groups']['dynamic_ancillary_file_group']['landcover_file'] = \
                os.path.join(args['landcover_file'])

    runconfig['runconfig']['groups']['dynamic_ancillary_file_group']['worldcover_file'] = \
                os.path.join(args['worldcover_file'])

    runconfig['runconfig']['groups']['dynamic_ancillary_file_group']['shoreline_shapefile'] = \
                os.path.join(args['shoreline_shapefile'])

    runconfig['runconfig']['groups']['product_path_group']['scratch_path'] = \
                os.path.join(granule_dir_path, 'scratch_dir')

    runconfig['runconfig']['groups']['product_path_group']['output_dir'] = \
                os.path.join(granule_dir_path, 'output_dir')

    runconfig['runconfig']['groups']['product_path_group']['product_path'] = \
                os.path.join(granule_dir_path, 'product_path')

    product_id_str = build_product_id_str(granule_dir_path, granule_id, list_of_urls)

    runconfig['runconfig']['groups']['product_path_group']['product_id'] = \
                product_id_str    

    # save into the granule's directory
    runconfig_path = os.path.join(granule_dir_path,'dswx_hls_runconfig.yaml')
    with open(runconfig_path, 'w') as rc:
        yaml.dump(runconfig, rc)

    return runconfig_path


def build_product_id_str(granule_dir_path, granule_id, list_of_urls):
    '''
    Per the DSWx-HLS Product Spec for Cal-Val (July 22, 2022), the desired Format of final product name:
    Project_Level_ProductType_Source_Sensor_PixelSpacing_TileID_SceneDateTime_
    ProductGenerationDateTime_ProductVersion_LayerNumber_LayerName.tif
    Example: 
    OPERA_DSWx-HLS_S2A_30_T15SXR_20210205T163901Z_20220101T140222Z_v0.1_B01_WTR.tif

    PROTEUS will handle creating and appending the '_v0.1_B01_WTR.tiff' substring. This function generates
    the first part of that string.
    Project: OPERA
    Level: L3
    ProductType: DSWx
    Source: The input source of the product (HLS in this case) 
    Sensor: The image input (Sentinel-2A [S2A], Sentinel-2B [S2B], or Landsat 8 [L8])
    PixelSpacing: Pixel spacing in meters
    TileID: Specific tile ID of the product, which is also the tile ID of the input HLS product in the MGRS
    SceneDateTime: The acquisition date and time (Greenwich Mean Time or GMT) of the center of the scene that 
        provides the input satellite imagery for this product (format: YYYYMMDDTHHMMSSZ)
    ProductGenerationDateTime: The date and time (GMT) at which the product was generated by OPERA (format: YYYYMMDDTHHMMSSZ)
    ProductVersion: OPERA DSWx-HLS product version number with four characters, including the letter “v” and two digits indicating the major and minor versions, which are delimited by a period
    LayerNumber: Three characters corresponding to the letter “B,” followed by a two-digit integer indicating the DSWx-HLS layer number, starting with 01 for the WTR layer 
    LayerName: Name of the DSWx-HLS layer (see Table 4-1)
    '''
    product_id = "OPERA_DSWx-HLS"

    ## Open one of the HLS input files in order to parse its metadata
    example_hls_filename = os.path.basename(list_of_urls[0])
    downloaded_hls_filename = os.path.join(granule_dir_path, 'input_dir', example_hls_filename)
    f = gdal.Open(downloaded_hls_filename)
    metadata = f.GetMetadata()
    # Close the file
    del f

    satellite, tileID, date = utility.get_sensor_tileID_date(granule_id)

    if satellite == "S30":
        # Sentinel metadata has a SPACECRAFT_NAME attribute
        if metadata['SPACECRAFT_NAME'] == 'Sentinel-2A':
            product_id += "_S2A"
        elif metadata['SPACECRAFT_NAME'] == 'Sentinel-2B':
            product_id += "_S2B"
        else:
            warnings.warn(f"Granule ID {granule_id}'s metadata could not be parsed to determine the Satellite.")

    elif satellite == "L30":
        # Landsat metadata has a SPACECRAFT_NAME attribute
        if metadata['LANDSAT_PRODUCT_ID'].split('_')[0] == 'LC08':
            product_id += "_L8"
        else:
            warnings.warn(f"Granule ID {granule_id}'s metadata could not be parsed to determine the Satellite.")
    else:
        warnings.warn(f"Granule ID {granule_id}'s metadata could not be parsed to determine the Satellite.")

    # Append the pixel spacing in meters
    product_id += "_30"

    # Append the TileID
    product_id += "_" + tileID

    ## After determining that the metadata does not contain Scene Center information
    # for both Landsat and Sentinel, instead we'll just copy over the time from the
    # input HLS granule's filename.

    # # Get SceneDateTime
    # # Note that the Granule ID's Date-Time is the time of the start of the acquisition,
    # # not the scene center time.
    # sceneDateTime = dswx_hls._get_avg_sensing_time(metadata['SENSING_TIME'])

    # # Append the SceneDateTime
    # product_id += "_" + sceneDateTime

    # Get HHMMSS
    # Example Granule ID: 'HLS.L30.T11TLH.2020160T183142.v2.0'
    hhmmss = granule_id.split('.')[3]
    hhmmss = hhmmss.split('T')[1]

    # Append the AcquisitionDateTime
    product_id += "_" + date + "T" + hhmmss + "Z"

    # Append the ProductGenerationDateTime
    product_id += "_" + utility.get_current_utc_time()

    return product_id
