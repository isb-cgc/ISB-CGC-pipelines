import os
import uuid
import argparse
from lib.pipelines.schema import PipelineSchema


def generate(args, config):
	parser = argparse.ArgumentParser()
	parser.add_argument("--inputFile", required=True)
	parser.add_argument("--tag", required=True)
	parser.add_argument("--diskSize", required=True)
	parser.add_argument("--outputPath", required=True)
	parser.add_argument("--logsPath", required=True)
	parser.add_argument("--preemptible", action="store_true", default=False, required=False)
	args = parser.parse_args(args=args)

	fastqcSchema = PipelineSchema('fastqc', args.tag, config)

	fastqcDiskName = "fastqc-{uuid}".format(uuid=str(uuid.uuid4()).split('-')[0])
	fastqcDiskType = "PERSISTENT_SSD"
	fastqcDiskMountPath = "/fastqc"
	fastqcSchema.addDisk(fastqcDiskName, fastqcDiskType, args.diskSize, fastqcDiskMountPath)

	inputFileName = os.path.basename(args.inputFile)
	fastqcSchema.addInput("inputFile", fastqcDiskName, inputFileName, args.inputFile)
	fastqcSchema.addOutput("output", fastqcDiskName, "{tag}*".format(tag=args.tag), args.outputPath)
	fastqcSchema.setLogOutput(args.logsPath)

	ext = inputFileName.split('.')[-1]
	inputFileBase = '.'.join(inputFileName.split('.')[0:-1])

	if ext == "fastq" or ext == "bam":
		fastqcCmd = (
			'cd {diskMountPath} && '
			'fastqc {inputFileName} && '
			'mv {inputFileBase}_fastqc.zip {tag}-{inputFileBase}_fastqc.zip && '
			'mv {inputFileBase}_fastqc.html {tag}-{inputFileBase}_fastqc.html'
		).format(diskMountPath=fastqcDiskMountPath, inputFileName=inputFileName, tag=args.tag, inputFileBase=inputFileBase)

	elif ext == "tar":
		fastqcCmd = (
			'cd {diskMountPath} && '
			'tar -tf {inputFileName} > {tag}-{inputFileName}.contents && '
			'tar -xf {inputFileName} && '
			'cat {tag}-{inputFileName}.contents | xargs fastqc && '
			'cat {tag}-{inputFileName}.contents | rev | cut -d \\".\\" -f 2- | rev | xargs -I \'{}\' mv \'{}\'_fastqc.zip {tag}-\'{}\'_fastqc.zip && '
			'cat {tag}-{inputFileName}.contents | rev | cut -d \\".\\" -f 2- | rev | xargs -I \'{}\' mv \'{}\'_fastqc.html {tag}-\'{}\'_fastqc.html && '
		).format(diskMountPath=fastqcDiskMountPath, inputFileName=inputFileName, tag=args.tag)

	elif ext == "gz": 
		fastqcCmd = (
			'cd {diskMountPath} && '
			'tar -tzf {inputFileName} > {tag}-{inputFileName}.contents && '
			'tar -xzf {inputFileName} && '
			'cat {tag}-{inputFileName}.contents | xargs fastqc && '
			'cat {tag}-{inputFileName}.contents | rev | cut -d \\".\\" -f 2- | rev | xargs -I \'{}\' mv \'{}\'_fastqc.zip {tag}-\'{}\'_fastqc.zip && '
			'cat {tag}-{inputFileName}.contents | rev | cut -d \\".\\" -f 2- | rev | xargs -I \'{}\' mv \'{}\'_fastqc.html {tag}-\'{}\'_fastqc.html && '
		).format(diskMountPath=fastqcDiskMountPath, inputFileName=inputFileName, tag=args.tag)

	fastqcSchema.setCmd(fastqcCmd)
	fastqcSchema.setImage("b.gcr.io/isb-cgc-public-docker-images/fastqc")

	fastqcSchema.setMem(2)
	fastqcSchema.setCpu(1)

	fastqcSchema.setLogOutput(args.logsPath)

	if args.preemptible:
		fastqcSchema.setPreemptible(True)

	return fastqcSchema
