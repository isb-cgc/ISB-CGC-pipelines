# ISB-CGC-pipelines Usage Examples

This directory contains code demonstrating a few approaches for using the ISB-CGC-pipelines framework.

## API Examples

The ISB-CGC-pipelines CLI is built on a relatively simple API which can be used directly to generate and run a DAG of tasks.  The [api](https://github.com/isb-cgc/ISB-CGC-pipelines/tree/master/lib/examples/api) subdirectory contains the following files:
- *-DAG.py : Python scripts using the ISB-CGC-pipelines API to construct and run pipelines
- launch-*-DAG.sh : Bash scripts that can be used to invoke the corresponding *-DAG.py file
- steps.py : A file containing example classes which define particular types of pipeline tasks, imported by the *-DAG.py files
- containers.py : A file for defining variables for containers, imported by the *-DAG.py files
- container_scripts.py : A file for defining variables for GCS URLs to container scripts, imported by the *-DAG.py files

## Example Container Scripts

The [container_scripts](https://github.com/isb-cgc/ISB-CGC-pipelines/tree/master/lib/examples/container_scripts) subdirectory contains a few sample container scripts that can be used to run a task using ISB-CGC-pipelines.  Simply copy one of these scripts to cloud storage and note the path for use with the CLI or API.

## Example Launch Scripts and Utility Scripts

The [launch_scripts](https://github.com/isb-cgc/ISB-CGC-pipelines/tree/master/lib/examples/launch_scripts) subdirectory contains an example Bash script ("startFastqc.sh") that demonstrates how to run an "embarrassingly parallel" task using an input manifest file ("tenBamFiles.txt") with the ISB-CGC-pipelines CLI.

The [utility_scripts](https://github.com/isb-cgc/ISB-CGC-pipelines/tree/master/lib/examples/utility_scripts) subdirectory contains a few utility scripts that can be used to more easily access/generate metadata about files for parametrization.
