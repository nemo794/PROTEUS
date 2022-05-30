## HLS Scaling Script Runconfig Parameters ##

## Study Area Settings

# (Store these into an args dict. This matches the API from utility.parse_args() .)
args = {}

# Data output setup:
args['root_dir'] = '.'                      # Directory to save the outputs to. e.g. "." for current directory
args['job_name'] = 'StudyArea'              # e.g. "HLSScalingJob". This will be the folder name in root_dir where all outputs are saved.

args['do_not_download'] = False             # True to only output the .txt lists of which granules meet the search query; 
                                            # will not download nor process the tiles.

args['do_not_process'] = False              # True to only download the granules meet the search query; will not process them through PROTEUS.

args['rerun'] = False                       # True to will use the existing

# Search Query Filters
args['bounding_box'] = '-120 43 -118 48'    # Area to observe. Format is W Longitude,S Latitude,E Longitude,N Latitude.
                                            # Coordinates are enclosed in quotes and separated by a space.
                                            # Ex: "-120 43 -118 48"

args['date_range'] = '2021-08-13/2021-08'   # Either a single datetime or datetime range used to filter results.
                                            # Examples: "2020-06-02/2021-06-30", "2021-08-13/2021-08", "2020"
                                            # For details on allowable dates, see the 'datetime' parameter
                                            # in: https://pystac-client.readthedocs.io/en/latest/api.html#stac-api-io
                                            # (This scaling script uses pystac-client's API to query STAC, so the input format is the same.)

args['months'] = 'Jun,Jul,Aug'              # String of Select Months within the date_range to search.
                                            # This should be a subset of "Jan,Feb,Mar,Apr,May,Jun,Jul,Aug,Sep,Oct,Nov,Dec", within the date_range specified.
                                            # Only use the first three letters of the desired months, and separate with a comma.
                                            # For any/all months, provide a empty string: ''

args['cloud_cover_max'] = 30                # 0-100; The maximum percentage of the image with cloud cover.
                                            # 0 => no clouds present, 100 => images could be fully covered in clouds

args['spatial_coverage_min'] = 40           # 0-100; The minimum percentage of the tile with data.
                                            # 0 => image has no pixels with satellite data, 100 => all pixels have satellite data

args['same_day'] = True                     # True to get only granules where both S30 and L30 images were collected on the same date


## Advanced Settings

# Runconfig Template file
# In the current template, all values surrounded by $$<VALUE>$$ will be
# replaced with the appropriate paths by the scaling script.
# To alter other items (such as the output DSWx-HLS products),
# please edit those fields in the template file before running the scaling script.
RUNCONFIG_TEMPLATE = './scaling/dswx_hls_runconfig_template.txt'

# World-wide dem.vrt file. This dem is used for all tiles processed.
# Note: in the future, if a world-wide dem file is no longer available
# and individual dem files must be fetched for each tile, then the
# workflow for populating each granule's runconfig file will need to be updated.
# See: desired_granules.py > create_runconfig_yaml()
DEM_FILE = '/home/shiroma/dat/nisar-dem-copernicus/EPSG4326.vrt'

# Bands to download for each satellite.
# For PROTEUS, the following bands are required:
#   L30: B02, B03, B04, B05, B06, B07, Fmask
#   S30: B02, B03, B04, B8A, B11, B12, Fmask
l30_v2_bands = ['B02',  # blue
                'B03',  # green
                'B04',  # red
                'B05',  # nir
                'B06',  # swir1
                'B07',  # swir2
                'Fmask'  # qa
                ]

s30_v2_bands = ['B02',  # blue
                'B03',  # green
                'B04',  # red
                'B8A',  # nir
                'B11',  # swir1
                'B12',  # swir2
                'Fmask'  # qa
                ]

# STAC Server to query.
# For HLS, use LPCLOUD: https://cmr.earthdata.nasa.gov/stac/LPCLOUD/
STAC_URL_LPCLOUD = 'https://cmr.earthdata.nasa.gov/stac/LPCLOUD/'

# Collections to query.
# To query HLS v2.0 L30 and S30 tiles, set equal to: ['HLSL30.v2.0', 'HLSS30.v2.0']
COLLECTIONS = ['HLSL30.v2.0', 'HLSS30.v2.0']
