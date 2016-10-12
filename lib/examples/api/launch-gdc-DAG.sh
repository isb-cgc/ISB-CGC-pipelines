#!/bin/bash

# $1 = local path to a GDC manifest file
# $2 = GCS URL of a valid GDC token file
# $3 = GCS directory URL for storing the GDC data
# $4 = prefix for naming FastQC output files
# $5 = GCS directory URL for storing the FastQC data
# $6 = GCS directory for storing logs

# read each line of a GDC manifest file to launch instances of the GDC DAG example
while read line; do
    read fileId fileName fileSize <<< $(echo "$line" | cut -d $'\t' -f 1,7,8)
    diskSize=$(calculateDiskSize --fileSize $fileSize --scalingFactor 2 --roundToNearestGbInterval 100)

    python gdc-DAG.py --gdc-file-uuid $fileId \
                      --gdc-token-url $2 \
                      --gdc-data-destination $3 \
                      --fastqc_output_prefix $4 \
                      --fastqc_data_destination $5 \
                      --logs_destination $6 \
                      --disk_size $diskSize \
                      --tag $fileId \
                      --preemptible
done < $1