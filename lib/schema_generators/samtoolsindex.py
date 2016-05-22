import os
import uuid
import argparse
from lib.pipelines.schema import PipelineSchema


def generate(args, config):
	parser = argparse.ArgumentParser()
	parser.add_argument("--bamFile", required=True, help="")
	parser.add_argument("--tag", required=True)
	parser.add_argument("--diskSize", required=True)
	parser.add_argument("--outputPath", required=True, help="")
	parser.add_argument("--logsPath", required=True, help="")
	parser.add_argument("--preemptible", required=False, action="store_true", default=False)

	args = parser.parse_args(args=args)

	samtoolsIndexSchema = PipelineSchema('samtoolsindex', args.tag, config)

	samtoolsIndexDiskName = "samtools-index-{uuid}".format(uuid=str(uuid.uuid4()).split('-')[0])
	samtoolsIndexDiskType = "PERSISTENT_SSD"
	samtoolsIndexDiskMountPath = "/samtools-index"
	samtoolsIndexSchema.addDisk(samtoolsIndexDiskName, samtoolsIndexDiskType, args.diskSize, samtoolsIndexDiskMountPath)

	bamFileName = os.path.basename(args.bamFile)
	baiFileName = "{f}.bai".format(f=bamFileName)

	samtoolsIndexSchema.addInput("bamFile", samtoolsIndexDiskName, bamFileName, args.bamFile)
	samtoolsIndexSchema.addOutput("baiFile", samtoolsIndexDiskName, baiFileName, args.outputPath)
	samtoolsIndexSchema.setLogOutput(args.logsPath)

	samtoolsIndexSchema.setCmd("cd {diskMountPath} && samtools index {bamFileName}".format(diskMountPath=samtoolsIndexDiskMountPath, bamFileName=bamFileName))
	samtoolsIndexSchema.setImage("biodckr/samtools")

	samtoolsIndexSchema.setMem(2)
	samtoolsIndexSchema.setCpu(1)

	if args.preemptible:
		samtoolsIndexSchema.setPreemptible(True)

	return samtoolsIndexSchema
