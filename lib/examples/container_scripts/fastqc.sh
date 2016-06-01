#!/bin/bash

# REQUIRED ENVIRONMENT VARIABLES:
# - INPUT_FILENAME
#
# OPTIONAL ENVIRONMENT VARIABLES:
# - OUTPUT_PREFIX
#
# CONTAINER IMAGE: b.gcr.io/isb-cgc-public-docker-images/fastqc

INPUT_FILE_BASE=$(echo $INPUT_FILENAME | rev | cut -d / -f 1 | cut -d . -f 2- | rev)
EXT=$(echo $INPUT_FILENAME | rev | cut -d . -f 1 | rev)

if [[ ! -z ${OUTPUT_PREFIX+x} ]]; then
	OUTPUT_PREFIX="${OUTPUT_PREFIX}-"
fi

if [[ "${EXT}" == "fastq" || "${EXT}" == "bam" ]]; then
	fastqc $INPUT_FILENAME && 
	mv "${INPUT_FILE_BASE}_fastqc.zip" "${OUTPUT_PREFIX}${INPUT_FILE_BASE}_fastqc.zip"
	mv "${INPUT_FILE_BASE}_fastqc.html" "${OUTPUT_PREFIX}${INPUT_FILE_BASE}_fastqc.html"

elif [[ "${EXT}" == "tar" ]]; then
	tar -tf $INPUT_FILENAME > "${OUTPUT_PREFIX}${INPUT_FILENAME}.contents"
	tar -xf $INPUT_FILENAME 
	cat "${OUTPUT_PREFIX}${INPUT_FILENAME}.contents" | xargs fastqc
	cat "${OUTPUT_PREFIX}${INPUT_FILENAME}.contents" | rev | cut -d "." -f 2- | rev | xargs -I {} mv {}_fastqc.zip $OUTPUT_PREFIX{}_fastqc.zip
	cat "${OUTPUT_PREFIX}${INPUT_FILENAME}.contents" | rev | cut -d "." -f 2- | rev | xargs -I {} mv {}_fastqc.html $OUTPUT_PREFIX{}_fastqc.html
	
elif [[ "${EXT}" == "gz" ]]; then 
	tar -tzf $INPUT_FILENAME > "${OUTPUT_PREFIX}-${INPUT_FILENAME}.contents" 
	tar -xzf $INPUT_FILENAME 
	cat "${OUTPUT_PREFIX}${INPUT_FILENAME}.contents" | xargs fastqc
	cat "${OUTPUT_PREFIX}${INPUT_FILENAME}.contents" | rev | cut -d "." -f 2- | rev | xargs -I {} mv {}_fastqc.zip $OUTPUT_PREFIX{}_fastqc.zip
	cat "${OUTPUT_PREFIX}${INPUT_FILENAME}.contents" | rev | cut -d "." -f 2- | rev | xargs -I {} mv {}_fastqc.html $OUTPUT_PREFIX{}_fastqc.html

fi
