import os
from setuptools import setup
from setuptools import Command

__version__ = version = VERSION = '0.1'

long_description = ''

class CleanCommand(Command):
    """Custom clean command to tidy up the project root."""
    user_options = []
    def initialize_options(self):
        pass
    def finalize_options(self):
        pass
    def run(self):
        # Make sure to remove the .egg-info file 
        os.system('rm -vrf .scratch_dir ./build ./dist ./*.pyc ./*.tgz ./*.egg-info ./src/*.egg-info')

package_data_dict = {}

package_data_dict['proteus'] = [
    os.path.join('defaults', 'dswx_hls.yaml'),
    os.path.join('schemas', 'dswx_hls.yaml'),
    os.path.join('examples', 'dswx_hls_scaling_runconfig.json')]

setup(
    name='proteus',
    version=version,
    description='Compute Dynamic Surface Water Extent (DSWx)'
                ' from optical (HLS) and SAR data',
    # package_dir is the directory where the egg_base (proteus.egg-info) will be stored.
    # Ex: If package_dir={'': 'src'}, then proteus.egg-info will be stored in the `src` directory.
    #     This also means that all python packages must be in the `src` directory;
    #     the `bin`` directory will no longer be an importable package.
    package_dir={'proteus': 'src/proteus'},
    packages=['proteus',
              'proteus.extern',
              'proteus.scaling'],
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
    cmdclass={
        'clean': CleanCommand,
    }
)