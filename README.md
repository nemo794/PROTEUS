# PROTEUS
PROTEUS - Parallelized Radar Optical Toolbox for Estimating dynamic sUrface water extentS

# License
**Copyright (c) 2021** California Institute of Technology (“Caltech”). U.S. Government
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

Download the source code and move working directory to clone repository:

```bash
git clone https://github.com/opera-adt/PROTEUS.git
cd PROTEUS
```

Install PROTEUS via conda/setup.py (recommended):

```bash
conda install --file docker/requirements.txt
conda install -c conda-forge --file docker/requirements.txt.forge
python setup.py install
```

Or via pip:

```bash
pip install .
```

Or via environment path setup:

```bash
export PROTEUS_HOME=$PWD
export PYTHONPATH=${PYTHONPATH}:${PROTEUS_HOME}/src
export PATH=${PATH}:${PROTEUS_HOME}/bin
```

Run workflow and unit tests:

```bash
cd tests
pytest -rpP
```

## HLS 2.0 Scaling Script

The DSWx-HLS Scaling Script queries NASA's STAC-CMR database for HLS 2.0 granules, filters the results based on given parameters, downloads the matching HLS granules, and processes them through PROTEUS.

### Installation

Installation Instructions:

1. Install PROTEUS by following the directions above. Make sure the tests pass.
2. Install pystac-client:
```conda install -c conda-forge pystac-client```
3. Setup your NASA Earthdata credentials store them in a netrc file
- A [NASA Earthdata Login Account](https://urs.earthdata.nasa.gov/) is required to download HLS data.
- The .netrc file will allow Python scripts to log into any Earthdata Login without being prompted for credentials every time you run. The .netrc file should be placed in your HOME directory. (On UNIX, do ```echo $HOME``` for the home directory.)
- To create your .netrc file, download the [.netrc file template](https://git.earthdata.nasa.gov/projects/LPDUR/repos/daac_data_download_python/browse/.netrc), update with your Earthdata login credentials, and save to your home directory.
4. cd into the hls_scaling directory. ```cd hls_scaling```
5. Use the hls_scaling_script.py. A tutorial with examples can be found below.


### Usage

Use -h or --help for available input options (with formats) and filter options.

All outputs will be placed into a new directory: ```<root_dir>/<job_name>```. If the HLS granules are downloaded and processed, a nested directory structure will be created inside that new directory and populated with the downloaded HLS granule files and output files from PROTEUS.

#### Command Line Tutorial

First, use --help to see the available API commands available. The help text contains a description of the input, the required format for the input, and example input(s).

```python hls_scaling_script.py --help```

Next, try a basic request via the Command Line. A request requires (at minimum) --root_dir, --job_name, --bbox, and --date_range. This example should produce 170 granules; once the console output says that downloading and processing have begun, wait a couple of seconds and then Ctrl-C to interupt the execution.

```python hls_scaling_script.py -–root_dir . --job_name StudyAreaTest --bbox '-120 43 -118 48' --date_range '2021-08-13/2021-08'```

Interrupting the process prevented the complete download and processing of the 170 tiles. However, because we waited until the downloading had begun, the search results from the query were still saved to ./StudyTestArea. These auto-generated files give the user insight into the query results, and will be needed to rerun this query in the future. , as well as a complete directory structure and whatever files were downloaded before the process was terminated. Of interest, the file ```settings.json``` contains the settings that were used to run this request, so that a user has a record of the study area, filters, and parameters used to get these query results.

Next, apply filters to that same search to narrow the STAC query results to 6 granules (see --help for options and format):

```python hls_scaling_script.py -–root_dir . --job_name StudyAreaTest --bbox '-120 43 -118 48' --date_range '2021-08-13/2021-08' --months 'Jun,Jul,Aug' --cloud_cover_max 30 --spatial_coverage 40 --same_day```

With an institution-based internet connection and multicore processor (8+ core), this can complete in 7-8 minutes. Slower connections and CPUs will take longer. Also notice that because this filtered request had the same root_dir and job_name as our first basic query, the script automatically appends a number to the end of job_name, and the results populated into a new directory: ./StudyAreaTest1. (This is so the first directory is not accidentally overwritten.) Now, take a moment to explore the outputs in the nested folders of ./StudyAreaTest1 and see where the results ended up.

Now, let's try that same command again. This time, let it download for ~20 seconds, then Ctrl-C the process to interrupt its execution.

```python hls_scaling_script.py -–root_dir . --job_name StudyAreaTest --bbox '-120 43 -118 48' --date_range '2021-08-13/2021-08' --months 'Jun,Jul,Aug' --cloud_cover_max 30 --spatial_coverage 40 --same_day```

Similar to before, it has begun storing its results in a new directory: ./StudyAreaTest2. Nested in this directory are `input_dir` directories where the HLS granule files are downloaded; some of the HLS granule files should be downloaded into each input_dir directory, but not all 7 needed for execution of PROTEUS.

To resume this process, use the ```--rerun``` command, making sure to specify the correct directory name:

```python hls_scaling_script.py –root_dir . --job_name StudyAreaTest2 --rerun```

Rerun will not re-download HLS granule files that have already been downloaded, but it will download any files remaining to be downloaded and then process those through PROTEUS. Note that Rerun requires that the main directory contain the files ```settings.json``` and ```study_results.pickle```, which were generated during the initial request.

##### Skip Download and Processing in PROTEUS via ```--do_not_download``` and ```--do_not_process```

To only output the filtered search results and skip downloading and processing the granules, include the ```--do_not_download``` flag in the command line.

To only download the HLS granules but not process them in PROTEUS, include the ```--do_not_process``` flag.

If at a later time the downloading and/or processing steps are desired for this granule, use ```--rerun```. By default, --rerun will both download and process the granules, but these defaults can be overwritten ```--do_not_download``` and ```--do_not_process```. (Although nothing )

##### Verbose flag ```-v``` or ```--verbose```

Use the ```--verbose``` flag to be more verbose in the outputs. This will also display the error messages from PROTEUS.
- Caution: The downloading and processing portion of the script uses threading and parallel processing, so console outputs can become intermingled and unwieldy, particularly when there are more than a handful of granules.

#### Running from a scaling runconfig .json file using ```--scaling_runconfig```

As an alternative to entering a series of command line arguments, a user can instead use the ```--scaling_runconfig``` option. This uses a human-readble and editable .json file to specify the setup for a run.

For this example, we'll use the example runconfig file located in the ```scaling``` directory, but in practise, the file can be located anywhere.

```python hls_scaling_script.py --scaling_runconfig ./scaling/scaling_runconfig_example.json```

The formats of the values in the runconfig file are identical to the formats of the inputs for command line parsing. However, unlike the command line, there are no default values provided when using the runconfig option; all fields are required to have a value.

Of interest, the format of this input file is identical to the auto-generated ```settings.json``` file that is saved to each new study area directory. So if you want to re-use the same settings as a previous request, or make a subtle change, then you can use that existing ```settings.json``` file as a template, make a copy of it and modify it with your change, and then run the new request.
