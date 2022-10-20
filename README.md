# PROTEUS
PROTEUS - Parallelized Radar Optical Toolbox for Estimating dynamic sUrface water extentS

# License
**Copyright (c) 2022** California Institute of Technology (“Caltech”). U.S. Government
sponsorship acknowledged.

All rights reserved.

Redistribution and use in source and binary forms, with or without modification, are permitted provided
that the following conditions are met:
* Redistributions of source code must retain the above copyright notice, this list of conditions and
the following disclaimer.
* Redistributions in binary form must reproduce the above copyright notice, this list of conditions
and the following disclaimer in the documentation and/or other materials provided with the
distribution.
* Neither the name of Caltech nor its operating division, the Jet Propulsion Laboratory, nor the
names of its contributors may be used to endorse or promote products derived from this software
without specific prior written permission.

THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS
IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO,
THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR
PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR
CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL,
EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO,
PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS;
OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY,
WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR
OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF
ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.

## Installation

### Download the Source Code
Download the source code and change working directory to cloned repository:

```bash
git clone https://github.com/nasa/PROTEUS.git
cd PROTEUS
```

### Standard Installation
Install dependencies (installation via conda is recommended):
```bash
conda install --file docker/requirements.txt
conda install -c conda-forge --file docker/requirements.txt.forge
```

Install via setup.py:

```bash
python setup.py install
python setup.py clean
```

Note: Installation via pip is not currently recommended due to an
issue with the osgeo and gdal dependency.


OR update environment path to run PROTEUS:

```bash
export PROTEUS_HOME=$PWD
export PYTHONPATH=${PYTHONPATH}:${PROTEUS_HOME}/src
export PATH=${PATH}:${PROTEUS_HOME}/bin
```

Run workflow tests to ensure proper installation:

```bash
pytest -rpP tests
```

Process data sets; use a runconfig file to specify the location
of the dataset, the output directory, parameters, etc.

```bash
dswx_hls.py <path to runconfig file>
```

A default runconfig file can be found: `PROTEUS > src > proteus > defaults > dswx_hls.yaml`.
This file can be copied and modified for your needs.
Note: The runconfig must meet this schema: `PROTEUS > src > proteus > schemas > dswx_hls.yaml`.


### Alternate Installation: Docker Image

Skip the standard installation process above.

Then, from inside the cloned repository, build the Docker image:
(This will automatically run the workflow tests.)

```bash
./build_docker_image.sh
```

Load the Docker container image onto your computer:

```bash
docker load -i docker/dockerimg_proteus_cal_val_3.1.tar
```

See DSWx-HLS Science Algorithm Software (SAS) User Guide for instructions on processing via Docker.


## DSWx-HLS 2.0 Scaling Script

The DSWx-HLS Scaling Script queries NASA's STAC-CMR database for HLS 2.0 granules, filters the results based on given parameters, downloads the matching HLS granules, and processes them through PROTEUS.

### Installation

Installation Instructions:

1. Install PROTEUS. The instructions above will install PROTEUS from the original repo. To install the scaling script, install from this fork:
```bash
git clone TODO
```


2. Setup your NASA Earthdata credentials, and store them in a netrc file. (Required to download HLS granules.)
- A [NASA Earthdata Login Account](https://urs.earthdata.nasa.gov/) is required to download HLS data.
- If the Earthdata credentials are invalid, the NASA CMR-STAC search query can still be completed. However, the HLS granule data cannot be downloaded.
- The .netrc file will allow Python scripts to log into any Earthdata Login without being prompted for credentials every time you run. The .netrc file should be placed in your HOME directory. (On UNIX, do ```echo $HOME``` for the home directory.)
- To create your .netrc file, download the [.netrc file template](https://git.earthdata.nasa.gov/projects/LPDUR/repos/daac_data_download_python/browse/.netrc), update with your Earthdata login credentials, and save to your home directory. If you already have a .netrc file, then please update it to also include your Earthdata credentials.


### Usage

Use -h or --help for available input options (with formats) and filter options.

All outputs will be placed into a new directory: ```<root-dir>/<job-name>```. If the HLS granules are downloaded and processed, a nested directory structure will be created inside that new directory and populated with the downloaded HLS granule files and output files from PROTEUS.

#### Command Line Tutorial

First, use --help to see the available API commands available. The help text contains a description of the input, the required format for the input, and example input(s).

```dswx_scaling_script.py --help```

Next, try a basic request via the Command Line. A request requires (at minimum) `--root-dir`, `--job-name`, `--bbox`, and `--date-range`. This example should produce 170 granules; once the console output says that downloading and processing have begun, wait a couple of seconds and then Ctrl-C to interupt the execution.

```dswx_scaling_script.py --root-dir . --job-name StudyAreaTest --bbox '-120 43 -118 48' --date-range '2021-08-13/2021-08'```

Interrupting the process prevented the complete download and processing of the 170 tiles. However, because we waited until the downloading had begun, the search results from the query were still saved to ./StudyTestArea. These auto-generated files give the user insight into the query results, and will be needed to rerun this query in the future. , as well as a complete directory structure and whatever files were downloaded before the process was terminated. Of interest, the file ```settings.json``` contains the settings that were used to run this request, so that a user has a record of the study area, filters, and parameters used to get these query results.

Next, apply filters to that same search to narrow the STAC query results to 6 granules (see --help for options and format):

```dswx_scaling_script.py --root-dir . --job-name StudyAreaTest --bbox '-120 43 -118 48' --date-range '2021-08-13/2021-08' --months 'Jun,Jul,Aug' --cloud-cover-max 30 --spatial-coverage 40 --same-day```

With an institution-based internet connection and multicore processor (8+ core), this can complete in 7-8 minutes. Slower connections and CPUs will take longer. Also notice that because this filtered request had the same root-dir and job-name as our first basic query, the script automatically appends a number to the end of job-name, and the results populated into a new directory: ./StudyAreaTest1. (This is so the first directory is not accidentally overwritten.) Now, take a moment to explore the outputs in the nested folders of ./StudyAreaTest1 and see where the results ended up.

Now, let's try that same command again. This time, let it download for ~20 seconds, then Ctrl-C the process to interrupt its execution.

```dswx_scaling_script.py --root-dir . --job-name StudyAreaTest --bbox '-120 43 -118 48' --date-range '2021-08-13/2021-08' --months 'Jun,Jul,Aug' --cloud-cover-max 30 --spatial-coverage 40 --same-day```

Similar to before, it has begun storing its results in a new directory: ./StudyAreaTest2. Nested in this directory are `input_dir` directories where the HLS granule files are downloaded; some of the HLS granule files should be downloaded into each `input_dir` directory, but not all 7 needed for execution of PROTEUS.

To resume this process, use the ```--rerun``` command, making sure to specify the correct directory name:

```dswx_scaling_script.py --root-dir . --job-name StudyAreaTest2 --rerun```

Rerun will not re-download HLS granule files that have already been downloaded, but it will download any files remaining to be downloaded and then process those through PROTEUS. Note that Rerun requires that the main directory contain the files ```settings.json``` and ```study_results.pickle```, which were generated during the initial request.

##### Skip Download and Processing in PROTEUS via ```--do-not-download``` and ```--do-not-process```

To only output the filtered search results and skip downloading and processing the granules, include the ```--do-not-download``` flag in the command line.

To only download the HLS granules but not process them in PROTEUS, include the ```--do-not-process``` flag.

If at a later time the downloading and/or processing steps are desired for this granule, use ```--rerun```. By default, `--rerun` will both download and process the granules, but these defaults can be overwritten ```--do-not-download``` and ```--do-not-process```.

##### Verbose flag ```-v``` or ```--verbose```

Use the ```--verbose``` flag to be more verbose in the outputs. This will also display the stdout messages from DSWx-HLS.
- Caution: The downloading and processing portion of the script uses threading and parallel processing, so console outputs can become intermingled and unwieldy, particularly when there are more than a handful of granules.

##### Viewing `stdout` and `stderr` from dswx_hls.py

When processing through DSWx-HLS, then each granule is processed independently and the stdout is automatically saved into each granule's directory as the log file `dswx_hls_log.txt`. To see the stderr generated from DSWx-HLS printed to console, use the `--verbose` flag.

#### Running from a scaling runconfig .json file using ```--scaling-runconfig```

As an alternative to entering a series of command line arguments, a user can instead use the ```--scaling-runconfig``` option. This uses a human-readable and editable .json file to specify the setup for a run.

For this example, we'll use the example runconfig file located in the ```examples``` directory, but in practise, the file can be located anywhere.

```dswx_scaling_script.py --scaling-runconfig ./src/proteus/examples/scaling_runconfig_example.json```

The formats of the values in the runconfig file are identical to the formats of the inputs for command line parsing. However, unlike the command line, there are no default values provided when using the runconfig option; all fields are required to have a value.

Of interest, the format of this input file is identical to the auto-generated ```settings.json``` file that is saved to each new study area directory. To re-use the same (or similar) settings as a previous study area request, simply make a copy of that .json file, and modify it with any changes, and then use the updated .json for the new request.
