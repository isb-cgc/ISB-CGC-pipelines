# PREREQUISITES FOR RUNNING THE TASKS BELOW:
# - Global configuration has been set using `isb-cgc-pipelines config ...`
# - Ansible has been installed locally on the workstation
#
# Plan: translate bash commands below into an Ansible playbook, which can be run to bootstrap
# the system based on the values in the global configuration.
# 
# General Algorithm:
# - Create a pubsub topic to be used as a sink for (filtered) compute engine logs (one each for insertions, deletions, and preemptions)
#

# Create a bucket for the cloud functions (eventually put functions in github)
gsutil mb ...

# Ensure that the cloud function code is in the specified bucket


# Create a Pub/Sub topic for messages originating from the pipeline server
curl -X "PUT" https://pubsub.googleapis.com/v1/projects/isb-cgc-data-02/topics/pipeline-server-logs

# Deploy a function for handling the messages originating from the pipeline server
gcloud alpha functions deploy pipelineServerLogs --bucket isb-cgc-data-02-functions --trigger-topic pipeline-server-logs

# Create a Pub/Sub topic for stdout logs originating from the pipeline jobs
curl -X "PUT" https://pubsub.googleapis.com/v1/projects/isb-cgc-data-02/topics/pipeline-job-stdout-logs

# Deploy a function for handling the stdout messages from pipeline jobs
gcloud alpha functions deploy pipelineJobStdoutLogs --bucket isb-cgc-data-02-functions --trigger-topic pipeline-job-stdout-logs

# Create a Pub/Sub topic for stderr logs originating from the pipeline jobs
curl -X "PUT" https://pubsub.googleapis.com/v1/projects/isb-cgc-data-02/topics/pipeline-job-stderr-logs

# Deploy a function for handling the stderr messages from pipeline jobs
gcloud alpha functions deploy pipelineJobStderrLogs --bucket isb-cgc-data-02-functions --trigger-topic pipeline-job-stderr-logs

# Create a Pub/Sub topic for compute engine logs (insertions)
curl -X "PUT" https://pubsub.googleapis.com/v1/projects/isb-cgc-data-02/topics/pipeline-vm-insert

# Deploy a function for handling insertions
gcloud alpha functions deploy pipelineVmInsert --bucket isb-cgc-data-02-functions --trigger-topic pipeline-vm-insert

# Create a sink for the vm insertions
# destination: pubsub.googlapis.com/v1/projects/isb-cgc-data-02/topics/pipeline-vm-insert
# filter:
# - log = "compute.googleapis.com/activity_log"
# - structPayload["event_subtype"] = "compute.instances.insert"
# - metadata["labels"]["compute.googleapis.com/resource_name"] matches regex '^ggp-[0-9]*$'
# - "error" not in structPayload.keys()

# Create a Pub/Sub topic for compute engine logs (preemptions)
curl -X "PUT" https://pubsub.googlapis.com/v1/projects/isb-cgc-data-02/topics/pipeline-vm-preempted

# Deploy a function for handling preemptions
gcloud alpha functions deploy pipelineVmPreempted --bucket isb-cgc-data-02-functions --trigger-topic pipeline-vm-preempted

# Create a sink for the vm preemptions
# destination: pubsub.googlapis.com/v1/projects/isb-cgc-data-02/topics/pipeline-vm-preempted
# filter: 
# - log = "compute.googleapis.com/activity_log"
# - structPayload["event_subtype"] = "compute.instances.preempted"
# - metadata["labels"]["compute.googleapis.com/resource_name"] matches regex '^ggp-[0-9]*$'
# - "error" not in structPayload.keys()

# Create a Pub/Sub topic for compute engine logs (deletions)
curl -X "PUT" https://pubsub.googlapis.com/v1/projects/isb-cgc-data-02/topics/pipeline-vm-delete

# Deploy a function for handling deletions
gcloud alpha functions deploy pipelineVmDelete --bucket isb-cgc-data-02-functions --trigger-topic pipeline-vm-delete

# Create a sink for the vm deletions
# destination: pubsub.googlapis.com/v1/projects/isb-cgc-data-02/topics/pipeline-vm-delete
# filter: 
# - log = "compute.googleapis.com/activity_log"
# - structPayload["event_subtype"] = "compute.instances.delete"
# - metadata["labels"]["compute.googleapis.com/resource_name"] matches regex '^ggp-[0-9]*$'
# - "error" not in structPayload.keys()
