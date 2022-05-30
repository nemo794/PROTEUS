import os
import sys
import requests
import warnings
from datetime import datetime
import json
import pickle

import xml.etree.ElementTree as ET

from scaling import utility


class StudyAreaGranules(object):

    def __init__(self, item_collection, \
                    cloud_cover_max=100, \
                    months=[1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12]):
        """
        item_collection is what is returned from a pystac-client .get_all_items() search query.
        """
    
        self.granules_to_download = {}
        num_items_removed_for_select_months = 0

        for i in item_collection:
            # Filter out items that are not during the requested months
            if len(months) < 12:
                date = datetime.strptime(i.properties['datetime'], \
                        '%Y-%m-%dT%H:%M:%S.%fZ')  # 2021-01-09T19:03:23.352Z
                if date.month not in months:
                    num_items_removed_for_select_months += 1
                    continue
            
            # Filter out items that have too much cloud cover
            if cloud_cover_max < 100:
                if i.properties['eo:cloud_cover'] > cloud_cover_max:
                    continue

            # We've cleared the initial filters, add item to self.granules_to_download
            self.granules_to_download[str(i.id)] = i

        if len(months) < 12:
            print("Number of granules after filtering for only select months: ", \
                len(item_collection) - num_items_removed_for_select_months)

        if cloud_cover_max < 100:
            print("Number of granules after filtering for cloud coverage: ", \
                len(self.granules_to_download))

        self.exit_if_no_downloads()


    # Filter for tiles+dates with coverage by S30 and L30
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

        print("Number of granules after filtering for S30 and L30 on Same Day: ", len(self.granules_to_download))
        self.exit_if_no_downloads()


    def filter_spatial_coverage(self, spatial_coverage_min):
        for item_name in list(self.granules_to_download.keys()):

            # Download the full metadata for this granule to memory
            xml_data = requests.get(self.granules_to_download[item_name].assets['metadata'].href, allow_redirects=True)

            # create element tree object
            root = ET.fromstring(xml_data.content)
        
            # get spatial_coverage value
            spatial_coverage = 101  # max value is 100
            for attribute in root.iter('AdditionalAttribute'):
                if 'SPATIAL_COVERAGE' == attribute.find('Name').text:
                    spatial_coverage = int(attribute.find('Values').find('Value').text)
                    break

            if spatial_coverage > 100:
                msg = "The metadata .xml for Granule %s did not include SPATIAL_COVERAGE." % item_name
                warnings.warn(msg)

            # If spatial coverage is too low, then
            # remove this granuale from the items to be downloaded
            # If SPATIAL_COVERAGE value was not found, we will not remove the Granule from self.granules_to_download
            if spatial_coverage < spatial_coverage_min:
                del self.granules_to_download[item_name]

        print("Number of granules after filtering for spatial coverage: ", len(self.granules_to_download))
        self.exit_if_no_downloads()


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
            # json.dump(self.granules_to_download, f)


