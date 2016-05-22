import os
import uuid
import argparse
from lib.pipelines.schema import PipelineSchema


def generate(args, config):
	parser = argparse.ArgumentParser()
	parser.add_argument("--bamFile", required=True, help="")
	parser.add_argument("--reference", required=True, help="")
	parser.add_argument("--tag", required=True, help="")
	parser.add_argument("--diskSize", required=True, help="")
	parser.add_argument("--outputPath", required=True, help="")
	parser.add_argument("--logsPath", required=True, help="")
	parser.add_argument("--preemptible", required=False, action="store_true", default=False)

	args = parser.parse_args(args=args)

	picardSchema = PipelineSchema('picard', args.tag, config)

	picardDiskName = "picard-{uuid}".format(uuid=str(uuid.uuid4()).split('-')[0])
	picardDiskType = "PERSISTENT_SSD"
	picardDiskMountPath = "/picard"
	picardSchema.addDisk(picardDiskName, picardDiskType, args.diskSize, picardDiskMountPath)

	bamFileName = os.path.basename(args.bamFile)
	baiFileName = "{bam}.bai".format(bam=bamFileName)
	baiFileUrl = "{bam}.bai".format(bam=args.bamFile)

	referenceFileName = os.path.basename(args.reference)

	picardSchema.addInput("bamFile", picardDiskName, bamFileName, args.bamFile)
	picardSchema.addInput("baiFile", picardDiskName, baiFileName, baiFileUrl)
	picardSchema.addInput("referenceSeq", picardDiskName, referenceFileName, args.reference)
	picardSchema.addOutput("picardOutputs", picardDiskName, "{tag}*".format(tag=args.tag), args.outputPath)

	picardSchema.setImage("b.gcr.io/isb-cgc-public-docker-images/qctools")
	picardSchema.setCmd((
		'cd {diskMountPath} && '
		'java -jar /usr/picard/picard.jar CollectMultipleMetrics VALIDATION_STRINGENCY=LENIENT ASSUME_SORTED=true INPUT={filename} OUTPUT={analysisId}-{filename}.multiple_metrics PROGRAM=CollectAlignmentSummaryMetrics PROGRAM=CollectInsertSizeMetrics PROGRAM=CollectQualityYieldMetrics PROGRAM=QualityScoreDistribution REFERENCE_SEQUENCE={reference} && '
		'java -jar /usr/picard/picard.jar BamIndexStats VALIDATION_STRINGENCY=LENIENT INPUT={filename} > {analysisId}-{filename}.bamIndexStats.tsv'
	).format(diskMountPath=picardDiskMountPath, reference=referenceFileName, analysisId=args.tag, filename=bamFileName))

	picardSchema.setCpu(1)
	picardSchema.setMem(2)

	picardSchema.setLogOutput(args.logsPath)

	picardSchema.setPreemptible(args.preemptible)

	return picardSchema