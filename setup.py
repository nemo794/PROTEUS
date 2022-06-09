import os
from setuptools import setup

__version__ = version = VERSION = '0.1'

long_description = ''

package_data_dict = {}

package_data_dict['proteus'] = [
    os.path.join('src', 'proteus', 'defaults', 'dswx_hls.yaml'),
    os.path.join('src', 'proteus', 'schemas', 'dswx_hls.yaml'),
    os.path.join('src', 'proteus', 'examples', 'dswx_hls_scaling_runconfig.json')]

setup(
    name='proteus',
    version=version,
    description='Compute Dynamic Surface Water Extent (DSWx)'
                ' from optical (HLS) and SAR data',
    package_dir={'proteus': 'src/proteus'},
    packages=['proteus',
              'proteus.extern',
              'proteus.dswx_hls_scaling'],
    include_package_data=True,
    package_data=package_data_dict,
    classifiers=['Programming Language :: Python',],
    scripts=['bin/dswx_hls.py',
    	     'bin/dswx_compare.py',
             'bin/dswx_landcover_mask.py',
             'bin/dswx_scaling_script.py'],
    install_requires=['argparse', 'numpy', 'yamale',
                      'osgeo', 'scipy', 'pytest', 'requests',
                      'pystac-client'],
    url='https://github.com/opera-adt/PROTEUS',
    author='Gustavo H. X. Shiroma and Samantha C. Niemoeller',
    author_email=('gustavo.h.shiroma@jpl.nasa.gov'),
    license='Copyright by the California Institute of Technology.'
    ' ALL RIGHTS RESERVED.',
    long_description=long_description,
)