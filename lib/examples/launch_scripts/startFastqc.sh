#!/bin/bash
# This script can be used with the sample "tenBamFiles.tsv" file as input, i.e.,
#
# `./startFastqc.sh tenBamFiles.tsv`
#
# The script will loop over every file in the listing to generate task requests to the Google Genomics Pipelines API
#
# Make path substitutions where appropriate.

while read bamFile; do

    # ISB-CGC BAI files will always exist in the same directory as their BAM counterparts
	baiFile="${bamFile}.bai"
	bamFileName=$(echo $bamFile | rev | cut -d '/' -f 1 | rev)
	baiFileName="${bamFileName}.bai"

	# Provide an output directory for the FastQC outputs
	outputPath=<GCS-OUTPUT-PATH>
	
	# Provide a log directory for log outputs
	logPath=<GCS-LOG-PATH>

    # Use the "calculateDiskSize" utility script to calculate the size of the disk
    diskSize=$(../utility_scripts/calculateDiskSize --inputFile $bamFile --roundToNearestGbInterval 100)

    # Submit a task to the Google Genomics Pipelines API for the given BAM file
	isb-cgc-pipelines submit --pipelineName fastqc \
		--inputs "${bamFile}:${bamFileName},${baiFile}:${baiFileName}" \
		--outputs "*_fastqc.zip:${outputPath},*_fastqc.html:${outputPath}" \
		--scriptUrl gs://isb-cgc-open/compute-helpers/pipeline-scripts/fastqc.sh \
		--imageName b.gcr.io/isb-cgc-public-docker-images/fastqc \
		--cores 1 --mem 2 \
		--env INPUT_FILENAME="${bamFileName}" \
		--diskSize $diskSize \
		--logsPath $logPath \
		--preemptible
done < $1
