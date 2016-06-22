#!/bin/bash

# REQUIRED ENVIRONMENT VARIABLES:
# - INPUT_FILENAME
#
# OPTIONAL ENVIRONMENT VARIABLES:
# - OUTPUT_PREFIX
#
# CONTAINER IMAGE: b.gcr.io/isb-cgc-public-docker-images/qctools

mkdir tmp

if [[ ! -z ${OUTPUT_PREFIX+x} ]]; then
	OUTPUT_PREFIX="${OUTPUT_PREFIX}-"
fi

java -jar /usr/picard/picard.jar CollectMultipleMetrics VALIDATION_STRINGENCY=LENIENT ASSUME_SORTED=true INPUT=$INPUT_FILENAME OUTPUT="${OUTPUT_PREFIX}${INPUT_FILENAME}.multiple_metrics" PROGRAM=CollectInsertSizeMetrics PROGRAM=CollectQualityYieldMetrics PROGRAM=QualityScoreDistribution TMP_DIR=`pwd`/tmp
		
java -jar /usr/picard/picard.jar BamIndexStats VALIDATION_STRINGENCY=LENIENT INPUT=$INPUT_FILENAME TMP_DIR=`pwd`/tmp > "${OUTPUT_PREFIX}${INPUT_FILENAME}.bamIndexStats.tsv"
