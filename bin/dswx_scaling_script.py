#!/usr/bin/env python3

# Based on code by Matt Bonnema
# See also: https://nasa-openscapes.github.io/2021-Cloud-Hackathon/tutorials/02_Data_Discovery_CMR-STAC_API.html

import os
import json
import pickle
import sys
import warnings
import time
import copy

from proteus.scaling import utility
from proteus.scaling import study_area_granules
from proteus.scaling import download_and_process as dap
from proteus.scaling import args_setup

## Must-do's
# TODO: get "python setup.py install" working
# TODO: get "pip install ." working

## Features on-deck:
# TODO: allow user to choose the directory structure


def main(args):

    # Verify input args
    args_setup.verify_input_args(args)

    if not args['rerun']:

        # Reformat the args for this class
        prepped_args = args_setup.reformat_args(args)

        ## Create an object to hold the desired granules to download.
        study_area = study_area_granules.StudyAreaQuery(
                            collections = prepped_args['collections'], \
                            stac_url_lpcloud = prepped_args['stac_url_lpcloud'], \
                            bounding_box = prepped_args['bounding_box'], \
                            intersects = prepped_args['intersects'], \
                            granule_ids = prepped_args['granule_ids'], \
                            date_range = prepped_args['date_range'], \
                            tile_id = prepped_args['tile_id'], \
                            months = prepped_args['months'], \
                            spatial_coverage_min = prepped_args['spatial_coverage_min'], \
                            cloud_cover_max = prepped_args['cloud_cover_max'], \
                            same_day = prepped_args['same_day'], \
                            verbose = prepped_args['verbose'], \
                            l30_v2_bands = prepped_args['l30_v2_bands'], \
                            s30_v2_bands = prepped_args['s30_v2_bands']

                            )

        ## Filter the query results
        study_area.query_STAC_and_filter_results()

        ## Save results from query to disk.

        # Make new job directory for this Study Area to hold all outputs
        job_dir = utility.make_job_dir(args['root_dir'], args['job_name'])

        # If --intersects was used, copy the file into the job directory
        # and update the original args dictionary for future reruns.
        # Note: using shutil module for this copy would be cleaner,
        # but this method was chosen to avoid an additional import and
        # because the intersects geojson files are quite small.
        if args['intersects']:
            intersects_copy_path = '%s/%s' % (job_dir,os.path.basename(args['intersects']))
            with open(intersects_copy_path, 'w') as out_file:
                with open(args['intersects'], 'r') as in_file:
                    out_file.write(in_file.read())
            
            # Update original settings with new path to intersects file
            args['intersects'] = intersects_copy_path
    
        # Save the original input args to settings.json file, in case of future rerun
        settings_json = os.path.join(job_dir, 'settings.json')        
        with open(settings_json, 'w') as f: 
            f.write(json.dumps(args, indent=4))

        # Save the query results to output files
        study_area.save_query_results_to_output_files(job_dir)


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
                                args=prepped_args)


if __name__ == "__main__":
    args = args_setup.parse_args()
    main(args)
