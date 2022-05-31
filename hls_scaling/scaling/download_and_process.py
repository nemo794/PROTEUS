import os
import sys
import requests
import warnings
import subprocess
from multiprocessing.pool import ThreadPool
from itertools import repeat

import xml.etree.ElementTree as ET

from scaling import utility


'''
Notes on the implementation strategy:

By using ThreadPool, .starmap() will send each granule
to its own thread to run download_and_process_granule().
ThreadPool also handles the limiting of the number of 
threads (and later processes) created, so as not to overwhelm
the system with potentially thousands of requests at once.

Internally, download_and_process_granule() first downloads (in serial)
all of the necessary .tif files for that granule, and 
second it generates a PROTEUS runconfig.yaml file and
spawns a subprocess to process that granule in PROTEUS.

Threads are well-suited for downloading files, and limiting the
number of concurrent downloads to the number of threads should
help with considerations for bandwidth and mitigating
HTTP 429 "Too Many Requests" errors from LPDAAC. However, this is
(educated) rationale, and there could be room for improvement.

subprocess.run() will spawn a new process in the OS for each granule
to process it through PROTEUS.
Like any process, the OS will handle scheduling these either 
in parallel or concurrently based on available CPU resources.
A benefit of using subprocess.run() with a runconfig.yaml file
is that the runconfig script can be stored and reviewed at a 
later date.

Possible design changes:
* To call PROTEUS natively in Python and skip using the runconfig.yaml
file, two items need refactoring:
    1) process_granule()'s call to subprocess.run would need to be
    changed to the generate_dswx_layers(...)
    2) "mp.pool.ThreadPool" should be changed to "mp.Pool". This
    will cause each call to download_and_process_granule() to be
    spawned into a new process for the OS to manange. This change
    would likely have minimal impact on overall run time, but we loose 
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

    # For each granule, get list of URLs to download
    # Get the list of urls to download for this Granule
    all_lists_of_urls = \
        [utility.get_list_of_urls(query_results_dict, granule_id, args['l30_v2_bands'], args['s30_v2_bands']) \
            for granule_id in query_results_dict.keys()]

    # Create directory structure for all granules.
    # Save this list to a txt file.
    # Note: Do this before the pool operation to avoid race conditions
    # (e.g. duplicated directories).
    list_of_granule_dirs = []
    with open(os.path.join(job_dir,'granule_dir.txt'), 'a+') as f:
        for granule_id in query_results_dict.keys():
            sensor, tileID, date = utility.get_sensor_tileID_date(granule_id)
            granule_dir = create_dir_structure(job_dir,[date,tileID,sensor])

            list_of_granule_dirs.append(granule_dir)

            f.write("%s\n" % granule_dir)

        # remove final "\n" newline character
        utility.remove_last_char(f)

    # Limit the maximum number of simultaneous downloads and processes to
    # the number of available CPUs.
    pool = ThreadPool(os.cpu_count())

    # Downloaded and process each granule independently
    result = pool.starmap(download_and_process_granule, \
                    zip(list_of_granule_ids, \
                        list_of_granule_dirs, \
                        all_lists_of_urls, \
                        repeat(args)))
    if not all(result):
        warnings.warn("Some of the granules were not successfully processed in PROTEUS.")


def create_dir_structure(job_dir=".", list_of_subdirs=[]):
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
        print('Beginning Download of granule ID %s...' % granule_id)
        download_granule_data(list_of_urls, dir_path)
        print('Download of granule ID %s complete.' % granule_id)
    except Exception as e:
        print(e)
        print("ERROR: granule %s could not be downloaded and will not be processed." % granule_id)
        return False

    # Process granule through DSWx-HLS (PROTEUS)
    if args['do_not_process']:
        print("Granule %s finished downloading on PID %s" % (granule_id, os.getpid()))
    else:
        # Create runconfig file
        runconfig_path = create_runconfig_yaml(dir_path, \
                                                args['runconfig_template'], \
                                                args['dem_file']
                                                )

        # Remove any leftover files from output_dir and scratch_dir
        for f in os.listdir(os.path.join(dir_path, 'output_dir')):
            os.remove(os.path.join(os.path.join(dir_path, 'output_dir'), f))
        for f in os.listdir(os.path.join(dir_path, 'scratch_dir')):
            os.remove(os.path.join(os.path.join(dir_path, 'scratch_dir'), f))

        # Process through DSWx-HLS (PROTEUS)
        print('Beginning Processing of granule ID %s in PROTEUS...' % granule_id)
        process_granule(runconfig_path, granule_id)
        print('Processing of granule ID %s in PROTEUS complete.' % granule_id)

        # try:
        #     process_granule(runconfig_path, granule_id)
        # except Exception as e:
        #     print(e)
        #     return False
    
    return True


def download_granule_data(list_of_urls, dir_path):

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


def process_granule(runconfig_path, granule_id):

    # Process through PROTEUS
    # Note that PROTEUS must be alread installed on the system to run.
    # See: https://github.com/opera-adt/PROTEUS for instructions.
    print("Beginning processing of granule id: ", granule_id)
    p = subprocess.run(['dswx_hls.py', runconfig_path], \
                        capture_output=True, text=True
                        )
                        
    # If process resulted in an error, then do something
    if p.stderr:
        # TODO - Decide how to handle incomplete processing. Should we rerun PROTEUS?
        # Display output from PROTEUS to console. This is messy with multiple processes running.
        # print(p.stdout)
        # print(p.stderr)
        warnings.warn("Errors were generated from PROTEUS while processing granule id %s." % granule_id)
        return False
    else:
        print("Successfully processed granule id %s in PROTEUS." % granule_id)
        # Display output from PROTEUS to console. This is messy with multiple processes running.
        # print(p.stdout)
        return True

def create_runconfig_yaml(granule_dir_path, runconfig_template, dem_file):
    # Read in template
    with open(runconfig_template, 'r') as template:
        tmpl = template.read()

    # Replace the target strings
    tmpl = tmpl.replace('$$INPUT_DIR$$', os.path.join(granule_dir_path,'input_dir'))
    tmpl = tmpl.replace('$$DEM_FILE$$', dem_file)
    tmpl = tmpl.replace('$$SCRATCH_DIR$$', os.path.join(granule_dir_path,'scratch_dir'))        
    tmpl = tmpl.replace('$$OUTPUT_DIR$$', os.path.join(granule_dir_path,'output_dir'))

    # save into the granule's directory
    runconfig_path = os.path.join(granule_dir_path,'dswx_hls_runconfig.yaml')
    with open(runconfig_path, 'w') as runcofig:
        runcofig.write(tmpl)

    return runconfig_path

