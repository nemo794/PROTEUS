import os
import sys
import requests
from datetime import datetime
import pickle
import warnings
import json
import time

import xml.etree.ElementTree as ET
from pystac_client import Client  # conda install -c conda-forge pystac-client

from proteus.scaling import utility

class StudyAreaQuery(object):

    def __init__(self, **kwargs):
        """
        This class  

        """

        # Parse input kwargs
        self.collections = kwargs['collections']
        self.stac_url_lpcloud = kwargs['stac_url_lpcloud']
        self.bounding_box = kwargs['bounding_box']
        self.intersects = kwargs['intersects']
        self.granule_ids = kwargs['granule_ids']
        self.date_range = kwargs['date_range']
        self.tile_id = kwargs['tile_id']
        self.months = kwargs['months']
        self.spatial_coverage_min = kwargs['spatial_coverage_min']
        self.cloud_cover_max = kwargs['cloud_cover_max']
        self.same_day = kwargs['same_day']
        self.verbose = kwargs['verbose']
        self.l30_v2_bands = kwargs['l30_v2_bands']
        self.s30_v2_bands = kwargs['s30_v2_bands']

        # Create a dictionary to hold the granules that will be downloaded.
        # This can be populated by calling StudyAreaQuery.query_STAC_and_filter_results .
        self.granules_to_download = {}


    def query_STAC_and_filter_results(self):

        # Query NASA's STAC-CMR
        item_collection = self.query_STAC()

        # Filter the initial STAC results and add them to the dictionary of granules.
        self.filter_item_collection_and_populate_dict(item_collection)

        # Filter for spatial coverage, cloud coverage, same date, etc.
        self.filter_query_dict()


    def query_STAC(self):

        ## Query NASA's STAC-CMR
        print("Beginning query of NASA's STAC-CMR for available granules in date_range and study area...")
        catalog = Client.open(self.stac_url_lpcloud)

        # Setup the search query. References:
        # pystac-client API for search: https://github.com/stac-utils/pystac-client/blob/7bedea4c0b9a181656d4a891ccf6c02361da8415/pystac_client/item_search.py#L87
        # General STAC API: https://github.com/radiantearth/stac-api-spec/tree/master/item-search

        # Future work: Some STAC Catalogs allow direct querying of eo:cloud_cover, but this fails for HLS
        # See: https://pystac-client.readthedocs.io/en/latest/usage.html#query-filter
        # See: https://github.com/radiantearth/stac-api-spec/tree/master/fragments/query
        # If querying becomes functional in the future, then add one of the following lines to the search:
        # filter={'eo:cloud_cover': {'lte': '20'}}
        # filter=['eo:cloud_cover<=20']
        max_items = 5000
        if self.granule_ids:
            search = catalog.search(
                ids=self.granule_ids,
                max_items=max_items,
                method='POST'
                )
        elif self.bounding_box:
            search = catalog.search(
                collections=self.collections,
                max_items=max_items,
                bbox=self.bounding_box,
                datetime=self.date_range,
                method='POST'
                )
        # Use the intersects input
        else:
            with open(self.intersects, 'r') as f:
                roi = json.load(f)
            search = catalog.search(
                collections=self.collections,
                max_items=max_items,
                intersects=roi,
                datetime=self.date_range,
                method='POST'
                )

        for attempt in range(1,4):
            try:
                item_collection = search.get_all_items()
                break
            except Exception as e:
                if attempt < 3:
                    print("Exception caught: ", e)
                    warnings.warn:("(Attempt %d of 3) Could not connect to STAC server. Will sleep for 5 secs and try again.", attempt)
                    time.sleep(5)
                else:
                    print("Exception caught: ", e)
                    warnings.warn:("(Attempt %d of 3.) Could not connect to STAC server.  Exiting program.", attempt)
                    sys.exit()

        print("Number of granules in initial query of STAC (entire date_range): ", len(item_collection))
        if len(item_collection) == max_items:
            print("WARNING: STAC Server likely has more than %s results, but only %s were returned." % (max_items,max_items))

        return item_collection


    def filter_item_collection_and_populate_dict(self, item_collection):
        """
        item_collection is what is returned from a pystac-client .get_all_items() search query.
        """

        num_items_removed_for_select_months = 0
        num_items_removed_for_tile_id = 0
        
        for i in item_collection:
            # Filter out tiles that are not our target tile
            if self.tile_id:
                if self.tile_id not in str(i.id):
                    num_items_removed_for_tile_id += 1
                    continue

            # Filter out items that are not during the requested months
            if len(self.months) < 12:
                date = datetime.strptime(i.properties['datetime'], \
                        '%Y-%m-%dT%H:%M:%S.%fZ')  # 2021-01-09T19:03:23.352Z
                if date.month not in self.months:
                    num_items_removed_for_select_months += 1
                    continue

            # We've cleared the initial filters, add item to self.granules_to_download
            self.granules_to_download[str(i.id)] = i

        if self.tile_id:
            print("Number of granules after filtering for only the desired MGRS Tile ID: ", \
                len(item_collection) - num_items_removed_for_tile_id)

        if len(self.months) < 12:
            print("Number of granules after filtering for only select months: ", \
                len(item_collection) - num_items_removed_for_tile_id - num_items_removed_for_select_months)

        self.exit_if_no_downloads()


    def filter_query_dict(self):

        # To minimize the amount of metadata .xml files to download and parse (very slow), 
        # filter for "same day" before checking for spatial and cloud coverage.
        # Will need to re-check for "same day" after each.
        if self.same_day:
            self.filter_dict_for_S30_L30_sameDay()
            print("Number of granules after initial filter for S30 and L30 on Same Day: ", \
                        len(self.granules_to_download))
            self.exit_if_no_downloads()

        num_removed_landsat9 = 0
        num_removed_spatial = 0
        num_removed_cloud = 0

        for item_name in list(self.granules_to_download.keys()):

            # Download the full metadata for this granule to memory
            xml_data = requests.get(self.granules_to_download[item_name].assets['metadata'].href, \
                                        allow_redirects=True)
            # create element tree object
            root = ET.fromstring(xml_data.content)

            for attribute in root.iter('AdditionalAttribute'):
                attr_name = attribute.find('Name').text
                cur_len = len(self.granules_to_download)

                # In HLS Metadata .xml files, the order these fields appear is:
                #   LANDSAT_PRODUCT_ID (optional), CLOUD_COVERAGE, SPATIAL_COVERAGE
                # So, filter in that order so that the counters are accurate.

                # remove Landsat-9 granules
                elif attr_name == 'LANDSAT_PRODUCT_ID' and ".L30." in item_name:
                    if not self.no_landsat9_req_ok(attribute, item_name):
                        num_removed_landsat9 += 1
                        break

                # filter for cloud coverage
                elif attr_name == 'CLOUD_COVERAGE' and self.cloud_cover_max < 100:
                    if not self.cloud_coverage_req_ok(attribute, item_name):
                        num_removed_cloud += 1
                        break

                # filter for spatial coverage
                if attr_name == 'SPATIAL_COVERAGE' and self.spatial_coverage_min > 0:
                    if not self.spatial_coverage_req_ok(attribute, item_name):
                        num_removed_spatial += 1
                        break

            self.exit_if_no_downloads()

        # Display the effects of the filters
        print("Number of granules after removing any Landsat-9 granules: ", 
                len(self.granules_to_download) \
                - num_removed_landsat9
                )

        if self.cloud_cover_max < 100:
            print("Number of granules after filtering for cloud coverage: ", 
                    len(self.granules_to_download) \
                    - num_removed_landsat9 \
                    - num_removed_cloud
                    )

        if self.spatial_coverage_min > 0:
            print("Number of granules after filtering for spatial coverage: ", 
                    len(self.granules_to_download) \
                    - num_removed_landsat9 \
                    - num_removed_cloud \
                    - num_removed_spatial
                    )

        # ensure same-day criteria is still met
        if self.same_day:
            self.filter_dict_for_S30_L30_sameDay()
            print("Number of granules after re-filtering for S30 and L30 on Same Day: ", \
                    len(self.granules_to_download))
            self.exit_if_no_downloads()


    def spatial_coverage_req_ok(self, attribute, item_name):
        spatial_coverage = int(attribute.find('Values').find('Value').text)

        # If spatial coverage is too low, then
        # remove this granuale from the items to be downloaded
        if spatial_coverage < self.spatial_coverage_min:
            del self.granules_to_download[item_name]
            return False
        else:
            return True


    def cloud_coverage_req_ok(self, attribute, item_name):
        cloud_coverage = int(attribute.find('Values').find('Value').text)

        # If cloud coverage is too high, then
        # remove this granuale from the items to be downloaded
        if cloud_coverage > self.cloud_cover_max:
            del self.granules_to_download[item_name]
            return False
        else:
            return True


    # Filter to remove Landsat 9 granules
    # Landsat 9 granules from Jan 2022 will be included in HLS data.
    # DSWx-HLS only supports Landsat-8 granules.
    def no_landsat9_req_ok(self, attribute, item_name):
        prod_id = attribute.find('Values').find('Value').text

        # Keep Landsat-8 granules
        if prod_id.startswith('LC08'):
            return True
        # Remove Landsat 9 granules
        elif prod_id.startswith('LC09'):
            del self.granules_to_download[item_name]
            return False
        else:
            raise Exception("A granule that was not Landsat 8 nor Landsat 9 was found.")


    # Filter for granules+dates with coverage by both S30 and L30
    def filter_dict_for_S30_L30_sameDay(self):
        """
        Assumption: Sentinel and Landsat have 5+ day return time,
        so we will assume that each satellite will only ever have
        one unique observation for a given date_tileID.
        
        Idea behind (below) algorithm: The first time we encouter a date_tileID
        in self.granules_to_download, we will add it to a new dict, unmatched_granules.
        If we encounter the same date_tileID again in self.granules_to_download,
        then this means both S30 and L30 observations exist for the same date_tileID,
        and we will remove it from the unmatched_granules dict.
        Afterwards, the unmatched_granules dict will contain only the "unmatched" granules, 
        e.g. the granules where S30 and L30 did NOT observe the same date_tileID.
        Last, we will remove the unmatched granules from the self.granules_to_download.
        """

        unmatched_granules = {}
        for granule_id in list(self.granules_to_download.keys()):
            # HLS Naming Format: https://lpdaac.usgs.gov/data/get-started-data/collection-overview/missions/harmonized-landsat-sentinel-2-hls-overview/#hls-naming-conventions
            # Example Granule ID: 'HLS.L30.T11TLH.2020160T183142.v2.0'
            id_split = granule_id.split('.')
            tileID = id_split[2][1:]      # remove "T" from the beginning
            date = id_split[3][:7]   # keep only YYYYDDD
            date_tileID = date + tileID

            if date_tileID in list(unmatched_granules.keys()):
                del unmatched_granules[date_tileID]
            else:
                unmatched_granules[date_tileID] = granule_id

        for item in unmatched_granules.keys():
            del self.granules_to_download[unmatched_granules[item]]


    def exit_if_no_downloads(self):
        if len(self.granules_to_download) == 0:
            print("No granules match all filter requirements. Exiting.")
            sys.exit()


    def save_query_results_to_output_files(self, job_dir):
        self.save_all_granule_names_to_file(job_dir)
        if self.verbose:
            print(f'List of filtered granule names saved to {job_dir}/dswx_hls_filtered_granules.txt.')

        self.save_all_urls_to_file(job_dir)
        if self.verbose:
            print(f'List of urls saved to {job_dir}/dswx_hls_filtered_urls.txt.')

        # Pickle the dictionary containing the final results of the query + filtering.
        # This output will be used for --rerun requests in the future
        self.save_query_results_to_file(job_dir)
        if self.verbose:
            print(f'Copy of query results dictionary saved to {job_dir}/query_results.pickle. This will be required to use --rerun option in the future.')

        if not self.verbose:
            print(f'Query results saved in {job_dir}.')


    def save_all_granule_names_to_file(self, job_dir):
        with open(os.path.join(job_dir,"dswx_hls_filtered_granules.txt"), "w") as f:
            for granule_id in self.granules_to_download.keys():
                f.write(granule_id + "\n")

            # remove final "\n" newline character
            utility.remove_last_char(f)


    def save_all_urls_to_file(self, job_dir):
        with open(os.path.join(job_dir,"dswx_hls_filtered_urls.txt"), "w") as f:
            for granule_id in self.granules_to_download.keys():
                list_of_urls = utility.get_list_of_urls(self.granules_to_download, granule_id, \
                                                        self.l30_v2_bands, self.s30_v2_bands)
                for url in list_of_urls:
                    f.write(url + "\n")

            # remove final "\n" newline character
            utility.remove_last_char(f)

    def save_query_results_to_file(self, job_dir):
        with open(os.path.join(job_dir,"query_results.pickle"), "wb") as f:
            pickle.dump(self.granules_to_download, f)

