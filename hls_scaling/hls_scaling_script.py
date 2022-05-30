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
3) cd into the hls_scaling directory.
4) Run hls_scaling_script.py using the Command Line.
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


'''
'''
Timing tests
6 tiles:
real    7m17.187s
user    24m18.756s
sys     0m14.616s

170 tiles:
real    18m5.665s
user    1730m33.836s
sys     12m27.343s
'''
import os
import json
import pickle

from pystac_client import Client  # conda install -c conda-forge pystac-client

from scaling import utility
from scaling import study_area_granules
from scaling import download_and_process as dap
from scaling import args_setup

## Must-do's
# TODO: build in a delay-and-retry if the STAC query is rejected.
# TODO: fork repo, update README. Include instructions to install PROTEUS, pystac-client, and setup the .netrc file
# TODO: have params.py act as a runconfig file for the scaling script.
# TODO: redo error handling for PROTEUS processing. Maybe include "verbose" as input?
# TODO: rename "Study Area" to "query results" (or something like that)
# TODO: Double-check usage of "tile" vs "granule" in variable naming
# TODO: Double-check help text for all input arguments. (particularly --rerun)
# TODO: pretty-print to the settings.json file

## Nice-to-haves
# TODO: Have TileIDs as inputs.
# TODO: Cleanup the "months"; query desired months in a loop
# TODO: Metadata downloading/searching can be done via threading
# TODO: Have "intersects" be an alternative to bounding_box as input
# TODO: allow user to choose the directory structure
# TODO: instead of hard-coding the bands, allow this to be an input
# TODO: Modularize the xml-checking to allow for filters in addition to SPATIAL_COVERAGE


def main(args):

    # Verify
    args_setup.verify_args(args, raw_input=True)

    # Convert args to desired format
    args_setup.prepare_args(args)

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

        item_collection = search.get_all_items()

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

        # Save args to settings.json file, in case of future rerun
        settings_json = os.path.join(job_dir, 'settings.json')        
        with open(settings_json, 'w') as f: 
            json.dump(args, f)

        study_area.save_all_granule_names_to_file(job_dir)
        print(f'List of filtered granule names saved to {job_dir}/dswx_hls_filtered_granules.txt.')

        study_area.save_all_urls_to_file(job_dir, args['l30_v2_bands'], args['s30_v2_bands'])
        print(f'List of urls saved to {job_dir}/dswx_hls_filtered_urls.txt.')

        # Pickle the dictionary containing the final results of the query + filtering.
        # This output will be used for --rerun requests in the future
        study_area.save_query_results_to_file(job_dir)
        print(f'copy of query results dictionary saved to {job_dir}/query_results.pickle. This will be required to use --rerun option in the future.')


    ## Download tiles

    if args['do_not_download']:
        print("Download and processing were not requested. Exiting now.")

        if args['rerun']:
            print("When using --rerun, to download (and process) the query results, please edit this Study Area's existing settings.json file so that do_not_download (and do_not_process) set to False.")

        sys.exit()

    ## Download granules and populate the job directory with the outputs
    if args['rerun']:
        job_dir = os.path.join(args['root_dir'], args['job_name'])

        # Load prior settings from saved settings.json file into the args.
        # This will replace all other args inputs.
        settings_json = os.path.join(job_dir, 'settings.json')

        with open(settings_json, 'r') as f:
            args = json.load(f)

        # Make sure 'rerun' is still set to True
        args['rerun'] = True

        # Verify args that were loaded from settings.json
        args_setup.verify_args(args, raw_input=False)

        # Read in query_results.pickle
        pickle_file = os.path.join(job_dir,'query_results.pickle')
        with open(pickle_file, 'rb') as f:
            query_results = pickle.load(f)

    else:
        query_results = study_area.granules_to_download

    dap.download_and_process_granules( \
                                job_dir=job_dir, \
                                query_results_dict=query_results, \
                                args=args)


if __name__ == "__main__":
    args = args_setup.parse_args()
    main(args)
