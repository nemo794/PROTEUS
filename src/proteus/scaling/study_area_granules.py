import os
import sys
import requests
import warnings
from datetime import datetime
import pickle

import xml.etree.ElementTree as ET

from proteus.scaling import utility


class StudyAreaGranules(object):

    def __init__(self, item_collection, \
                    months=None):
        """
        item_collection is what is returned from a pystac-client .get_all_items() search query.
        """
    
        self.granules_to_download = {}
        num_items_removed_for_select_months = 0

        for i in item_collection:
            # Filter out items that are not during the requested months

            if months is not None and len(months) < 12:
                date = datetime.strptime(i.properties['datetime'], \
                        '%Y-%m-%dT%H:%M:%S.%fZ')  # 2021-01-09T19:03:23.352Z
                if date.month not in months:
                    num_items_removed_for_select_months += 1
                    continue
            
            # # Filter out tiles that are not our target tile
            # if "T11SQA" not in str(i.id):
            #     continue

            # We've cleared the initial filters, add item to self.granules_to_download
            self.granules_to_download[str(i.id)] = i

        if len(months) < 12:
            print("Number of granules after filtering for only select months: ", \
                len(item_collection) - num_items_removed_for_select_months)

        self.exit_if_no_downloads()

    def filter_query_results(self, args):

        # To minimize the amount of metadata .xml files to download and parse (very slow), 
        # filter for "same day" before checking for spatial and cloud coverage.
        # Will need to re-check for "same day" after each.
        if args['same_day']:
            self.filter_S30_L30_sameDay()
            print("Number of granules after initial filter for S30 and L30 on Same Day: ", \
                        len(self.granules_to_download))
            self.exit_if_no_downloads()

        for item_name in list(self.granules_to_download.keys()):

            # Download the full metadata for this granule to memory
            xml_data = requests.get(self.granules_to_download[item_name].assets['metadata'].href, \
                                        allow_redirects=True)

            # create element tree object
            root = ET.fromstring(xml_data.content)
        
            for attribute in root.iter('AdditionalAttribute'):
                attr_name = attribute.find('Name').text

                # filter for spatial coverage
                if attr_name == 'SPATIAL_COVERAGE' and args['spatial_coverage_min'] > 0:
                    self.filter_spatial_coverage(attribute, item_name, args['spatial_coverage_min'])

                # filter for cloud coverage
                elif attr_name == 'CLOUD_COVERAGE' and args['cloud_cover_max'] < 100:
                    self.filter_cloud_coverage(attribute, item_name, args['cloud_cover_max'])

                # remove Landsat-9 granules
                elif attr_name == 'LANDSAT_PRODUCT_ID' and ".L30." in item_name:
                    self.filter_landsat9(attribute, item_name)

            self.exit_if_no_downloads()

        print("Number of granules after spatial coverage, cloud coverage, and/or Landsat-9 filters: ", \
                len(self.granules_to_download))

        # ensure same-day criteria is still met
        if args['same_day']:
            self.filter_S30_L30_sameDay()
            print("Number of granules after re-filtering for S30 and L30 on Same Day: ", \
                    len(self.granules_to_download))
            self.exit_if_no_downloads()


    def filter_spatial_coverage(self, attribute, item_name, spatial_coverage_min):
        spatial_coverage = int(attribute.find('Values').find('Value').text)

        # If spatial coverage is too low, then
        # remove this granuale from the items to be downloaded
        if spatial_coverage < spatial_coverage_min:
            del self.granules_to_download[item_name]


    def filter_cloud_coverage(self, attribute, item_name, cloud_cover_max):
        cloud_coverage = int(attribute.find('Values').find('Value').text)

        # If cloud coverage is too high, then
        # remove this granuale from the items to be downloaded
        if cloud_coverage > cloud_cover_max:
            del self.granules_to_download[item_name]


    # Filter to remove Landsat 9 granules
    # Landsat 9 granules from Jan 2022 will be included in HLS data.
    # DSWx-HLS only supports Landsat-8 granules.
    def filter_landsat9(self, attribute, item_name):
        prod_id = attribute.find('Values').find('Value').text

        # Keep Landsat-8 granules
        if prod_id.startswith('LC08'):
            return
        # Remove Landsat 9 granules
        elif prod_id.startswith('LC09'):
            del self.granules_to_download[item_name]
        else:
            raise Exception("A granule that was not Landsat 8 nor Landsat 9 was found.")


    # Filter for granules+dates with coverage by both S30 and L30
    def filter_S30_L30_sameDay(self):
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


    def save_all_granule_names_to_file(self, job_dir):
        with open(os.path.join(job_dir,"dswx_hls_filtered_granules.txt"), "w") as f:
            for granule_id in self.granules_to_download.keys():
                f.write(granule_id + "\n")

            # remove final "\n" newline character
            utility.remove_last_char(f)


    def save_all_urls_to_file(self, job_dir, l30_v2_bands, s30_v2_bands):
        with open(os.path.join(job_dir,"dswx_hls_filtered_urls.txt"), "w") as f:
            for granule_id in self.granules_to_download.keys():
                list_of_urls = utility.get_list_of_urls(self.granules_to_download, granule_id, l30_v2_bands, s30_v2_bands)
                for url in list_of_urls:
                    f.write(url + "\n")

            # remove final "\n" newline character
            utility.remove_last_char(f)

    def save_query_results_to_file(self, job_dir):
        with open(os.path.join(job_dir,"query_results.pickle"), "wb") as f:
            pickle.dump(self.granules_to_download, f)

