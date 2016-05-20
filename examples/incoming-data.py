import os
import argparse
from pipelines.builder import PipelineBuilder
from pipelines.schema import PipelineSchema
from pipelines.utils import PipelinesConfig

# Parse Arguments
parser = argparse.ArgumentParser()
parser.add_argument("--analysisId", required=True)
parser.add_argument("--cghubKey", required=False)
parser.add_argument("--cghubOutput", required=True)
parser.add_argument("--cghubLogs", required=True)
parser.add_argument("--bbtFilters", required=True)
parser.add_argument("--bbtOutput", required=True)
parser.add_argument("--bbtLogs", required=True)
parser.add_argument("--fastqcOutput", required=True)
parser.add_argument("--fastqcLogs", required=True)
parser.add_argument("--samtoolsIndexLogs", required=True)
parser.add_argument("--setMetaLogs", required=True)
parser.add_argument("--diskSize", required=True)
parser.add_argument("--preemptible", action="store_true", default=False, required=False)
args = parser.parse_args()

# Build "Incoming Data" Pipeline

configPath = os.path.join(os.environ["HOME"], ".isb-cgc-pipelines", "config")

pipelinesConfig = PipelinesConfig(configPath)

pipelineBuilder = PipelineBuilder(pipelinesConfig)

setMetaFiles = []

## CGHUB STEP ##

from schema_generation.cghub import generate

cghubArgs = ["--analysisId", args.analysisId, "--cghubKey", args.cghubKey, "--diskSize", args.diskSize, "--logsPath", args.cghubLogs, "--outputPath", args.cghubOutput]

if args.preemptible:
	cghubArgs.append("--preemptible")

try:
	cghubSchema = generate(cghubArgs, pipelinesConfig) 
except LookupError as e:
	print "ERROR: Couldn't start pipeline for analysis ID {a}: {reason}".format(a=args.analysisId, reason=e)
	exit(-1)

pipelineBuilder.addStep(cghubSchema)

## DOWNSTREAM STEPS ##

setMetaParents = []

foundBai = False
foundBam = False
foundFastq = False

files = cghubSchema.getSchemaMetadata("files")
objectPath = cghubSchema.getSchemaMetadata("objectPath")

for f in files:
	ext = f["filename"].split('.')[-1]
	if ext == "bam":
		foundBam = True
		bamFileName = f["filename"]
	
	elif ext == "bai":
		foundBai = True
		baiFileName = f["filename"]

	elif ext == "fastq" or ext == "tar" or ext == "gz":
		foundFastq = True
		fastqFileName = f["filename"]
		fastqFileBase = '.'.join(f["filename"].split('.')[0:-1])

if foundBam:
	# BIOBLOOMCATEGORIZER STEP
	from schema_generation.biobloomcategorizer import generate

	bbcArgs = ["--bamFile", os.path.join(args.cghubOutput, objectPath, bamFileName), "--filters", args.bbtFilters, "--tag", args.analysisId, "--diskSize", args.diskSize, "--outputPath", os.path.join(args.bbtOutput, objectPath), "--logsPath", args.bbtLogs]

	if args.preemptible:
		bbcArgs.append("--preemptible")

	bbcSchema = generate(bbcArgs, pipelinesConfig) 
	
	setMetaFiles.append(os.path.join(args.cghubOutput, objectPath, bamFileName))

	pipelineBuilder.addStep(bbcSchema)
	cghubSchema.addChild(bbcSchema)

	setMetaParents.append(bbcSchema)

	if not foundBai:
		from schema_generation.samtoolsindex import generate

		samtoolsIndexArgs = ["--bamFile", os.path.join(args.cghubOutput, objectPath, bamFileName), "--tag", args.analysisId, "--diskSize", args.diskSize, "--outputPath", os.path.join(args.cghubOutput, objectPath), "--logsPath", args.samtoolsIndexLogs]

		if args.preemptible:
			samtoolsIndexArgs.append("--preemptible")

		samtoolsIndexSchema = generate(samtoolsIndexArgs, pipelinesConfig)

		baiFileName = "{bam}.bai".format(bam=os.path.basename(bamFileName))

		pipelineBuilder.addStep(samtoolsIndexSchema)
		cghubSchema.addChild(samtoolsIndexSchema)

		setMetaParents.append(samtoolsIndexSchema)

	setMetaFiles.append(os.path.join(args.cghubOutput, objectPath, baiFileName))


elif foundFastq:
	# FASTQC STEP
	from schema_generation.fastqc import generate

	fastqcArgs = ["--fastqFile", fastqFileName, "--tag", args.analysisId, "--outputPath", os.path.join(args.fastqcOutput, objectPath), "--diskSize", args.diskSize, "--logsPath", args.fastqcLogs]

	if args.preemptible:
		fastqcArgs.append("--preemptible")

	fastqcSchema = generate(fastqcArgs, pipelinesConfig)
	setMetaFiles.append(os.path.join(args.fastqcOutput, objectPath, "{analysisId}*".format(analysisId=analysisId))) # will glob patterns work with `gsutil setmeta ...`?
	pipelineBuilder.addStep(fastqcSchema)
	cghubSchema.addChild(fastqcSchema)
	setMetaParents.append(fastqcSchema)
	

# SETMETA STEP

setMetaSchema = PipelineSchema('setmeta', args.analysisId, pipelinesConfig)

setMetaSchema.setImage("google/cloud-sdk")
setMetaSchema.setCmd('gsutil setmeta -h \"x-goog-meta-analysis-id:{analysisId}\" {setMetaFiles}'.format(analysisId=args.analysisId, setMetaFiles=' '.join(setMetaFiles)))
setMetaSchema.setLogOutput(args.setMetaLogs)

for p in setMetaParents:
	p.addChild(setMetaSchema)

pipelineBuilder.addStep(setMetaSchema)

pipelineBuilder.run()	


