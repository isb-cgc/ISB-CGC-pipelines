#!/bin/bash

cp -R ../ISB-CGC-pipelines /usr/local/ISB-CGC-pipelines
ln -s /usr/local/ISB-CGC-pipelines/lib/isb-cgc-pipelines /usr/bin/isb-cgc-pipelines
ln -s /usr/local/ISB-CGC-pipelines/lib/scheduler/pipelineJobScheduler /usr/bin/pipelineJobScheduler
ln -s /usr/local/ISB-CGC-pipelines/lib/scheduler/receivePipelineVmLogs /usr/bin/receivePipelineVmLogs
ln -s /usr/local/ISB-CGC-pipelines/lib/examples/utility_scripts/calculateDiskSize /usr/bin/calculateDiskSize
ln -s /usr/local/ISB-CGC-pipelines/lib/examples/utility_scripts/constructCghubFilePaths /usr/bin/constructCghubFilePaths
ln -s /usr/local/ISB-CGC-pipelines/lib/examples/utility_scripts/getChecksum /usr/bin/getChecksum





