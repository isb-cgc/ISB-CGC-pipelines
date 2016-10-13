import os
import re
import requests
import argparse

import containers
import container_scripts
from steps import *
from pipelines.utils import PipelineBuilder, PipelinesConfig
from pipelines.paths import CLIENT_CONFIG_PATH


parser = argparse.ArgumentParser()
parser.add_argument("--fastq-archive-url", required=True)
parser.add_argument("--viral-sequence-dir", required=True)
parser.add_argument("--filters-dir", required=True)
parser.add_argument("--samtools-faidx-cores", required=False, default=1)
parser.add_argument("--samtools-faidx-mem", required=False, default=2)
parser.add_argument("--output-prefix", required=True)
parser.add_argument("--biobloommaker-cores", required=False, default=1)
parser.add_argument("--biobloommaker-mem", required=False, default=2)
parser.add_argument("--biobloomcategorizer-cores", required=False, default=1)
parser.add_argument("--biobloomcategorizer-mem", required=False, default=2)
parser.add_argument("--categorized-output-dir", required=True)
parser.add_argument("--logs-destination", required=True)
parser.add_argument("--disk-size", required=True)
parser.add_argument("--disk-type", required=False, default="PERSISTENT_SSD")
parser.add_argument("--tag", required=True)
parser.add_argument("--preemptible", action="store_true", default=False)

args = parser.parse_args()

config = PipelinesConfig(CLIENT_CONFIG_PATH)
p = PipelineBuilder(config)

# fqArchiveUrl, filtersDir, outputPrefix, outputUrl, diskSize, diskType, logsPath, container, scriptUrl, tag, cores, mem, preemptible
try:
	seqDirContents = subprocess.check_output(["gsutil", "ls", args.viral_sequence_dir])
except subprocess.CalledProcessError as e:
	print "ERROR: couldn't process viral sequences: {reason}".format(reason=e)
	exit(-1)

biobloommakerStep = BiobloommakerStep(args.viral_sequence_dir,
                                      args.output_prefix,
                                      args.filters_dir,
                                      args.disk_size,
                                      args.disk_type,
                                      args.logs_destination,
                                      containers.BBT_CONTAINER,
                                      container_scripts.BIOBLOOMMAKER_SCRIPT,
                                      args.tag,
                                      args.biobloommaker_cores,
                                      args.biobloomaker_mem,
                                      args.preemptible).get()

biobloomcategorizerStep = BiobloomcategorizerStep(args.fastq_archive_url,
                                                  args.filters_dir,
                                                  args.output_prefix,
                                                  args.categorized_output_dir,
                                                  args.disk_size,
                                                  args.disk_type,
                                                  args.logs_destination,
                                                  containers.BBT_CONTAINER,
                                                  container_scripts.BIOBLOOMCATEGORIZER_SCRIPT,
                                                  args.tag,
                                                  args.biobloomcategorizer_cores,
                                                  args.biobloomcategorizer_mem,
                                                  args.preemptible).get()

biobloommakerStep.addChild(biobloomcategorizerStep)

sequenceFiles = sorted(filter(lambda x: re.match('^.*\.fasta$', x) or re.match('^.*\.fa$', x) or re.match('^.*\.fai$', x), seqDirContents.split('\n')))
refMap = dict(filter(lambda (x, y): re.match('^.*\.fasta$', x) or re.match('^.*\.fa$', x), [(x,sequenceFiles[i+1]) if (re.match('^.*\.fasta$', x) or re.match('^.*\.fa$', x)) and re.match('^.*\.fai$', sequenceFiles[i+1]) else (x, None) for i, x in enumerate(sequenceFiles) if i + 1 < len(sequenceFiles)]))

for fa, fai in refMap.iteritems():
	if fai is None:
		samtoolsFaidxStep = SamtoolsFaidxStep(fa,
		                                      args.viral_sequence_dir,
		                                      args.disk_size,
		                                      args.disk_type,
		                                      args.logs_destination,
		                                      containers.SAMTOOLS_CONTAINER,
		                                      container_scripts.SAMTOOLS_FAIDX_SCRIPT,
		                                      args.tag,
		                                      args.samtools_faidx_cores,
		                                      args.samtools_faidx_mem,
		                                      args.preemptible).get()

		samtoolsFaidxStep.addChild(biobloommakerStep)
		p.addStep(samtoolsFaidxStep)

p.add(biobloommakerStep)
p.add(biobloomcategorizerStep)
p.run()
