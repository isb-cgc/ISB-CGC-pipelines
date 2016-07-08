#!/bin/bash

# TODO: command line options and error checking; otherwise find a way to read this info from the config file
CLUSTER_NAME=$1
CLUSTER_ZONE=$2
CLUSTER_DISK_SIZE=$3
CLUSTER_MACHINE_TYPE=$4
CLUSTER_NUM_NODES=$5
CLUSTER_SCOPES=$6
RESOURCE_DIR=$7
PROJECT_ID=$8

# check for gcloud and kubectl
gcloud_exists=$(which gcloud)
kubectl_exists=$(which kubectl)
if [[ -z "$gcloud_exists" ]]; then
	echo "ERROR: no command named 'gcloud' found on system... did you forget to install the Google Cloud SDK?"
	exit 1
elif [[ -z "$kubectl_exists" ]]; then
	echo "ERROR: no command named 'kubectl' found on system... did you forget to install kubectl?"
	exit 1
fi

# create a gcloud configuration for the pipelines service
gcloud config configurations create pipelines || true 
gcloud config configurations activate pipelines
gcloud config set project $PROJECT_ID
gcloud config set compute/zone $CLUSTER_ZONE

# create the cluster
gcloud container clusters create $CLUSTER_NAME \
	--zone $CLUSTER_ZONE \
	--disk-size $CLUSTER_DISK_SIZE \
	--machine-type $CLUSTER_MACHINE_TYPE \
	--num-nodes $CLUSTER_NUM_NODES \
	--scopes $CLUSTER_SCOPES \
	--wait || true

# get cluster credentials
kubectl config set cluster $CLUSTER_NAME
gcloud container clusters get-credentials $CLUSTER_NAME

# set the cluster in the kubectl configuration
kubectl config set cluster $CLUSTER_NAME 

# run kubectl in "proxy" mode
kubectl proxy --port 8080 &

cd $RESOURCE_DIR

# create the namespace
kubectl create -f namespace.json

# set the namespace in the kubectl configuration
context=$(kubectl config current-context)
kubectl config set "contexts.${context}.namespace" $CONTEXT_NAME

# create the services
for s in $(ls -1 *-service.json); do
	kubectl create -f $s
done

# create the replication controllers
for rc in $(ls -1 *-rc.json); do
	kubectl create -f $rc
done

# delete the resource dir
cd .. && rm -r $RESOURCE_DIR




