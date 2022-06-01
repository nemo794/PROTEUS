#!/usr/bin/env python3

# See scaling/args_setup.py for possible command line options. -h for help.

# Based on code by Matt Bonnema
# See also: https://nasa-openscapes.github.io/2021-Cloud-Hackathon/tutorials/02_Data_Discovery_CMR-STAC_API.html

# Outputs will populate into this default directory structure:
#       root_dir > ProjectStudyArea > Date > TileID > Sensor > [...]


'''
Quick Guide until the README is ready!

Installation Instructions:

1) Install PROTEUS. Follow the instructions on PROTEUS' README, and make sure the unit tests pass. Do not use the Docker version.
2) Install pystac-client.  ```conda install -c conda-forge pystac-client```
3) Setup a .netrc file (directions below)
4) cd into the hls_scaling directory.
5) Run hls_scaling_script.py using the Command Line.
    Example commands are given below. Use -h or --help for the available inputs.


Sample Runs from Command Line:
This uses all filters, which narrows the query results to 6 granules:

    python hls_scaling_script.py --root . --name StudyAreaTest --bbox '-120 43 -118 48' --date_range '2021-08-13/2021-08' --months 'Jun,Jul,Aug' --cloud_cover_max 30 --spatial_coverage 40 --same_day 

If you Ctrl-C and kill the scaling script mid-way through downloading, etc.
then you can "rerun" the identical Study Area search by providing three arguments:
1) the root directory
2) the name of the Study Area Project's directory in the root folder, which should contain some files from the first time it was run.
3) the "--rerun" flag

    python hls_scaling_script.py --root . --name StudyAreaTest --rerun


Setting up a .netrc file
Credit: HLS_SuPER_Script
Setting up a netrc File
Netrc files contain remote username/passwords that can only be accessed by the user of that OS (stored in home directory). Here we use a netrc file to store NASA Earthdata Login credentials. If a netrc file is not found when the script is executed, you will be prompted for your NASA Earthdata Login username and password by the script, and a netrc file will be created in your home directory. If you prefer to manually create your own netrc file, download the .netrc file template, add your credentials, and save to your home directory. A NASA Earthdata Login Account is required to download HLS data.

Link to .netrc template: https://git.earthdata.nasa.gov/projects/LPDUR/repos/daac_data_download_python/browse/.netrc
Link to setting up a NASA Earthdata Login Account: https://urs.earthdata.nasa.gov/
'''
import os
import json
import pickle
import sys
import warnings
import time
import copy

from pystac_client import Client  # conda install -c conda-forge pystac-client

from scaling import utility
from scaling import study_area_granules
from scaling import download_and_process as dap
from scaling import args_setup

## Must-do's
# TODO: fork repo, update README. Include instructions to install PROTEUS, pystac-client, and setup the .netrc file
# TODO: rename "Study Area" to "query results" (or something like that)

## Nice-to-haves
# TODO: Have TileIDs as inputs.
# TODO: Cleanup the "months" filter; query desired months in a loop. Should make the queries smaller/faster.
# TODO: Metadata downloading/searching can be done via threading
# TODO: Have "intersects" be an alternative to bounding_box as input
# TODO: allow user to choose the directory structure
# TODO: Modularize the xml-checking to allow for filters in addition to SPATIAL_COVERAGE


def main(args):

    # Verify
    args_setup.verify_input_args(args)

    # If not --rerun, make a copy of the original input arguments. (It is a small dictionary.)
    # Later, once the script has successfully completed the query and filtering,
    # a new directory for storing outputs will be created, and this copy will be stored there.
    if not args['rerun']:
        unprocessed_args = copy.deepcopy(args)

    # Reformat args for use by this script
    args_setup.reformat_args(args)

    if not args['rerun']:
        ## Query NASA's STAC-CMR
        print("Beginning query of NASA's STAC-CMR for available granules in date_range and study area...")
        catalog = Client.open(args['STAC_URL_LPCLOUD'])

        # STAC API: https://github.com/radiantearth/stac-api-spec/tree/master/item-search
        # pystac-client API for search: https://github.com/stac-utils/pystac-client/blob/7bedea4c0b9a181656d4a891ccf6c02361da8415/pystac_client/item_search.py#L87
        # (The pystac-client API is what is being used in this script.)
        search = catalog.search(
            collections=args['COLLECTIONS'],
            bbox=args['bounding_box'],
            datetime=args['date_range'],
            method='POST'
            # Some STAC Catalogs allow direct querying of eo:cloud_cover, but this fails for HLS
            # See: https://pystac-client.readthedocs.io/en/latest/usage.html#query-filter
            # See: https://github.com/radiantearth/stac-api-spec/tree/master/fragments/query
            # If querying becomes functional in the future, then uncomment one of the following lines:
            # filter={'eo:cloud_cover': {'lte': '20'}}
            # filter=['eo:cloud_cover<=20']
            )

        for attempt in range(1,4):
            try:
                item_collection = search.get_all_items()
            except APIError as e:
                if attempt < 3:
                    warnings.warn:("Could not connect to STAC server. Will sleep for 5 secs and try again. (Attempt %d of 3)", attempt)
                    time.sleep(5)
                else:
                    warnings.warn:("Could not connect to STAC server. Attempt %d of 3. Exiting program.", attempt)
                    sys.exit()

        print("Number of granules in initial query of STAC (entire date_range): ", len(item_collection))

        ## Create an object to hold the desired granules to download.
        # For efficiency of computation, filtering for cloud_cover_max and months
        # occurs during this step.
        study_area = study_area_granules.StudyAreaGranules(item_collection, \
                                            args['cloud_cover_max'], args['months'])

        # To minimize the amount of metadata .xml files to download and parse (very slow), 
        # filter for "same day" before checking for spatial coverage.
        # Will need to re-check for "same day" after.
        if args['same_day']:
            study_area.filter_S30_L30_sameDay()

        if args['spatial_coverage_min'] > 0:
            study_area.filter_spatial_coverage(args['spatial_coverage_min'])

            if args['same_day']:
                study_area.filter_S30_L30_sameDay()


        ## Save results from query to disk.

        # Make new job directory for this Study Area to hold all outputs
        job_dir = utility.make_job_dir(args['root_dir'], args['job_name'])

        # Save the original input args to settings.json file, in case of future rerun
        settings_json = os.path.join(job_dir, 'settings.json')        
        with open(settings_json, 'w') as f: 
            f.write(json.dumps(unprocessed_args, indent=4))

        study_area.save_all_granule_names_to_file(job_dir)
        if args['verbose']:
            print(f'List of filtered granule names saved to {job_dir}/dswx_hls_filtered_granules.txt.')

        study_area.save_all_urls_to_file(job_dir, args['l30_v2_bands'], args['s30_v2_bands'])
        if args['verbose']:
            print(f'List of urls saved to {job_dir}/dswx_hls_filtered_urls.txt.')

        # Pickle the dictionary containing the final results of the query + filtering.
        # This output will be used for --rerun requests in the future
        study_area.save_query_results_to_file(job_dir)
        if args['verbose']:
            print(f'Copy of query results dictionary saved to {job_dir}/query_results.pickle. This will be required to use --rerun option in the future.')

        if not args['verbose']:
            print(f'Query results saved in {job_dir}.')

    ## Download granules

    if args['do_not_download']:
        print("Download and processing were not requested. Exiting now.")
        sys.exit()

    if args['rerun']:
        job_dir = os.path.join(args['root_dir'], args['job_name'])

        # Read in query_results.pickle
        pickle_file = os.path.join(job_dir,'query_results.pickle')
        with open(pickle_file, 'rb') as f:
            query_results = pickle.load(f)

    else:
        query_results = study_area.granules_to_download

    ## Download granules and populate the job directory with the outputs
    dap.download_and_process_granules( \
                                job_dir=job_dir, \
                                query_results_dict=query_results, \
                                args=args)


if __name__ == "__main__":
    args = args_setup.parse_args()
    main(args)
