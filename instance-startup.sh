#!/bin/bash

echo 'deb http://www.rabbitmq.com/debian/ testing main' >> /etc/apt/sources.list.d/rabbitmq.list
wget -O- https://www.rabbitmq.com/rabbitmq-release-signing-key.asc | apt-key add -
apt-get -y update
apt-get -y install build-essential python-dev libffi-dev libssl-dev python-pip git wget sqlite3 supervisor rabbitmq-server
pip install -U virtualenv google-api-python-client pyinotify json-spec python-dateutil pika

groupadd supervisor
chgrp -R supervisor /etc/supervisor /var/log/supervisor
chmod -R 0775 /etc/supervisor /var/log/supervisor

cd /usr/local
git clone https://github.com/isb-cgc/ISB-CGC-pipelines.git

ln -s /usr/local/ISB-CGC-pipelines/lib/isb-cgc-pipelines /usr/bin/isb-cgc-pipelines
ln -s /usr/local/ISB-CGC-pipelines/lib/scheduler/pipelineJobScheduler /usr/bin/pipelineJobScheduler
ln -s /usr/local/ISB-CGC-pipelines/lib/scheduler/pipelineJobCanceller /usr/bin/pipelineJobCanceller
ln -s /usr/local/ISB-CGC-pipelines/lib/scheduler/receivePipelineVmLogs /usr/bin/receivePipelineVmLogs
ln -s /usr/local/ISB-CGC-pipelines/lib/examples/utility_scripts/calculateDiskSize /usr/bin/calculateDiskSize
ln -s /usr/local/ISB-CGC-pipelines/lib/examples/utility_scripts/constructCghubFilePaths /usr/bin/constructCghubFilePaths
ln -s /usr/local/ISB-CGC-pipelines/lib/examples/utility_scripts/getChecksum /usr/bin/getChecksum
ln -s /usr/local/ISB-CGC-pipelines/lib/examples/utility_scripts/getFilenames /usr/bin/getFilenames


