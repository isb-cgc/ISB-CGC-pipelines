#!/bin/bash

# REQUIRED ENVIRONMENT VARIABLES:
# - INPUT_FILENAME
# - REFERENCE_NAME
#
# OPTIONAL ENVIRONMENT VARIABLES:
# - OUTPUT_PREFIX
#
# CONTAINER IMAGE: b.gcr.io/isb-cgc-public-docker-images/qctools

if [[ ! -z ${OUTPUT_PREFIX+x} ]]; then
	OUTPUT_PREFIX="${OUTPUT_PREFIX}-"
fi

java -jar /usr/picard/picard.jar CollectMultipleMetrics VALIDATION_STRINGENCY=LENIENT ASSUME_SORTED=true INPUT=$INPUT_FILENAME OUTPUT="${OUTPUT_PREFIX}${INPUT_FILENAME}.multiple_metrics" PROGRAM=CollectAlignmentSummaryMetrics PROGRAM=CollectInsertSizeMetrics PROGRAM=CollectQualityYieldMetrics PROGRAM=QualityScoreDistribution REFERENCE_SEQUENCE=$REFERENCE_NAME
		
java -jar /usr/picard/picard.jar BamIndexStats VALIDATION_STRINGENCY=LENIENT INPUT=$INPUT_FILENAME > "${OUTPUT_PREFIX}${INPUT_FILENAME}.bamIndexStats.tsv"
