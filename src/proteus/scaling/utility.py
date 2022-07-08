import os
import argparse
from datetime import datetime, timezone
import requests
import warnings

from osgeo import gdal

def remove_last_char(f):
    '''
    f : a file handle to a text file. This function removes the 
        final character from the file.
        Example usage: removing an ending newline character.
    '''
    remove_chars = len(os.linesep)
    f.truncate(f.tell() - remove_chars)


def get_list_of_urls(granules_to_download_dict, granule_id, l30_v2_bands, s30_v2_bands):
                        
    # Get the list of urls to download for this granule_id
    list_of_urls = []
    sensor,_,_ = get_sensor_tileID_date(granule_id)
    if sensor == 'L30':  # Landsat L30 Granule
        for band in l30_v2_bands:
            list_of_urls += [granules_to_download_dict[granule_id].assets[band].href]

    else:  # Sentinel S30 Granule
        for band in s30_v2_bands:
            list_of_urls += [granules_to_download_dict[granule_id].assets[band].href]

    return list_of_urls


def valid_GeoTiff(file_name):
    try:
        gdal.Open(file_name)
    except:
        return False
    return True


def download2file(url, file_name):
    '''
    url       : (str) the url of the file to download
    file_name : (str) the filename (with path) of where to save the downloaded file

    Returns True if download completed, False if download had an error.
    
    Background info:
    HTTP codes range from the 1XX to 5XX, Common status codes:
    1XX - Information
    2XX - Success  (200 means the request was successful)
    3XX - Redirect
    4XX - Client Error (you made an error)
    5XX - Server Error (they made an error)

    If the status code is 4XX or 5XX, Python's Requests module will evaluate the response object to False.
    '''

    # Surround with try/catch so that a failed request does not kill the entire scaling script
    try:
        data = requests.get(url, allow_redirects=True)
    except Exception as e:
        warnings.warn(e)
        return False

    if data:
        open(file_name, 'wb').write(data.content)
        return True
    else:
        if data.status_code == 401:
            print(f"Error {data.status_code}. Check .netrc Earthdata credentials. Failed to download file {url}.")
        else: 
            print(f"Error {data.status_code}. Failed to download file {url}.")
        return False


def month_to_num(month_name):
    """month_name (string) must be one of: 
        "Jan","Feb","Mar","Apr","May","Jun","Jul","Aug","Sep","Oct","Nov", or "Dec"
    """
    return datetime.strptime(month_name, '%b').month


def get_sensor_tileID_date(granule_name):
        # HLS Naming Format: https://lpdaac.usgs.gov/data/get-started-data/collection-overview/missions/harmonized-landsat-sentinel-2-hls-overview/#hls-naming-conventions
        # Example Granule ID: 'HLS.L30.T11TLH.2020160T183142.v2.0'
        id_split = granule_name.split('.')
        sensor = id_split[1]    # 'L30' or 'S30'
        tileID = id_split[2]
        date = id_split[3][:7]   # keep only YYYYDDD

        # Convert YYYYDDD to YYYYMMDD
        date = datetime.strftime(datetime.strptime(date, '%Y%j'), '%Y%m%d')

        return sensor, tileID, date

def make_job_dir(root_dir, job_name):
    """
    Creates job directory and returns its file path.
    """
    job_dir = os.path.join(root_dir, job_name)
    run_num = 1
    while os.path.isdir(job_dir):
        job_dir = os.path.join(root_dir, "%s%s" % (job_name,run_num))
        run_num += 1
    os.makedirs(job_dir)
    return job_dir


def get_current_utc_time():
    '''
    Returns a string of the current UTC (GMT) time in the format: YYYYMMDDTHHMMSSZ
    datetime.timetuple() returns a tuple with the following entries:
        tm_year
        tm_mon
        tm_mday
        tm_hour
        tm_min
        tm_sec
        tm_wday
        tm_yday
        tm_isdst
    '''

    dt = datetime.now(timezone.utc).timetuple()

    # pad values with zeros
    return f'{dt.tm_year}{dt.tm_mon:02}{dt.tm_mday:02}T{dt.tm_hour:02}{dt.tm_min:02}{dt.tm_sec:02}Z'
