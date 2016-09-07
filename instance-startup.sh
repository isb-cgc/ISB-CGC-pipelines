#!/bin/bash

apt-get -y update
apt-get -y install build-essential python-dev libffi-dev libssl-dev python-pip git wget
pip install -U virtualenv google-api-python-client json-spec python-dateutil pyinotify pika Jinja2 futures

cd /usr/local
git clone https://github.com/isb-cgc/ISB-CGC-pipelines.git
cd ISB-CGC-pipelines && git fetch && git checkout -b v2_dev

ln -s /usr/local/ISB-CGC-pipelines/lib/isb-cgc-pipelines /usr/bin/isb-cgc-pipelines
ln -s /usr/local/ISB-CGC-pipelines/lib/examples/utility_scripts/calculateDiskSize /usr/bin/calculateDiskSize
ln -s /usr/local/ISB-CGC-pipelines/lib/examples/utility_scripts/constructCghubFilePaths /usr/bin/constructCghubFilePaths
ln -s /usr/local/ISB-CGC-pipelines/lib/examples/utility_scripts/getChecksum /usr/bin/getChecksum
ln -s /usr/local/ISB-CGC-pipelines/lib/examples/utility_scripts/getFilenames /usr/bin/getFilenames


