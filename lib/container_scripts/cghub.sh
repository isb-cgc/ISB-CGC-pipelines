#!/bin/bash

# REQUIRED ENVIRONMENT VARIABLES: 
# - ANALYSIS_ID
#
# OPTIONAL ENVIRONMENT VARIABLES:
# - CGHUB_KEY
#
# CONTAINER IMAGE: b.gcr.io/isb-cgc-public-docker-images/gtdownload

max_dl_attempts=3

if [[ ! -z ${CGHUB_KEY+x} ]]; then
	while [ "$max_dl_attempts" -ge 0 ]; do 
		if [ "$max_dl_attempts" -eq 0 ]; then 
			exit -1
		else 
			gtdownload -v --max-children 4 -k 60 -c $CGHUB_KEY -d $ANALYSIS_ID --path . 
			if [ "$?" -ne 0 ]; then 
				sleep 10
				max_dl_attempts=$(expr $max_dl_attempts - 1)
			else 
				break 
			fi 
		fi
	done
else
	while [ "$max_dl_attempts" -ge 0 ]; do 
		if [ "$max_dl_attempts" -eq 0 ]; then 
			exit -1
		else 
			gtdownload -v --max-children 4 -k 60 -d $ANALYSIS_ID --path . 
			if [ "$?" -ne 0 ]; then 
				sleep 10
				max_dl_attempts=$(expr $max_dl_attempts - 1)
			else 
				break 
			fi 
		fi
	done
fi
