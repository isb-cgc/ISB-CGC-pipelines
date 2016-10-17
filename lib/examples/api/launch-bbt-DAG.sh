#!/bin/bash

# $1 = GCS URL of a FASTQ archive file
# $2 = GCS URL of a directory containing a set of FASTA files; any FASTA without an accompanying index file will be indexed, and the index files will be copied to this directory
# $3 = GCS URL for the resultant bloom filter files
# $4 = an output prefix to use for naming output files (bloom filters and categorized reads)
# $5 = GCS URL for the resultant categorized reads
# $6 = GCS URL for log output
# $7 = a secondary (unique) identifier for this pipeline instance

python bbt-DAG.py --fastq-archive-url $1 \
                  --viral-sequence-dir $2 \
                  --filters-dir $3 \
                  --output-prefix $4 \
                  --categorized-output-dir $5 \
                  --logs-destination $6 \
                  --tag $7 \
                  --samtools-faidx-cores 1 \
                  --samtools-faidx-mem 2 \
                  --biobloommaker-cores 1 \
                  --biobloommaker-mem 2 \
                  --biobloomcategorizer-cores 1 \
                  --biobloomcategorizer-mem 2 \
                  --disk-size $(calculateDiskSize --inputFile $1 --scalingFactor 2 --roundToNearestGbInterval 100) \
                  --disk-type "PERSISTENT_SSD" \
                  --preemptible