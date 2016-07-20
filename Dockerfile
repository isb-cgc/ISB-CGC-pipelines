FROM debian:jessie
ENV debian_frontent NONINTERACTIVE
RUN apt-get -y update && apt-get -y install build-essential python-dev libffi-dev libssl-dev python-pip git sqlite3 && apt-get -y clean
RUN pip install -U virtualenv google-api-python-client pyinotify json-spec python-dateutil pika WebOb Paste webapp2
ADD . /usr/local/ISB-CGC-pipelines/
RUN cp /usr/local/ISB-CGC-pipelines/lib/k8s/docker/pipelineServer /usr/bin/pipelineServer
RUN cp /usr/local/ISB-CGC-pipelines/lib/scheduler/pipelineJobScheduler /usr/bin/pipelineJobScheduler
RUN cp /usr/local/ISB-CGC-pipelines/lib/scheduler/pipelineJobScheduler /usr/bin/pipelineJobScheduler
RUN cp /usr/local/ISB-CGC-pipelines/lib/scheduler/pipelineJobCanceller /usr/bin/pipelineJobCanceller
RUN cp /usr/local/ISB-CGC-pipelines/lib/scheduler/receivePipelineVmLogs /usr/bin/receivePipelineVmLogs
RUN cp /usr/local/ISB-CGC-pipelines/lib/examples/utility_scripts/calculateDiskSize /usr/bin/calculateDiskSize
RUN cp /usr/local/ISB-CGC-pipelines/lib/examples/utility_scripts/constructCghubFilePaths /usr/bin/constructCghubFilePaths
RUN cp /usr/local/ISB-CGC-pipelines/lib/examples/utility_scripts/getChecksum /usr/bin/getChecksum
RUN cp /usr/local/ISB-CGC-pipelines/lib/examples/utility_scripts/getFilenames /usr/bin/getFilenames
ENV PYTHONPATH /usr/local/lib/python2.7:/usr/local/ISB-CGC-pipelines/lib
ENV PIPELINES_CONFIG /etc/isb-cgc-pipelines/config
EXPOSE 80





