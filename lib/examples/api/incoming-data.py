import os
import argparse
from pipelines.builder import PipelineBuilder
from pipelines.schema import PipelineSchema
from pipelines.utils import PipelinesConfig, DataUtils

import pprint

# Parse Arguments
parser = argparse.ArgumentParser()
parser.add_argument("--analysisId", required=True)
parser.add_argument("--cghubKey", required=False)
parser.add_argument("--cghubOutput", required=True)
parser.add_argument("--cghubScript", required=True)
parser.add_argument("--cghubLogs", required=True)
parser.add_argument("--fastqcOutput", required=True)
parser.add_argument("--fastqcLogs", required=True)
parser.add_argument("--samtoolsIndexLogs", required=True)
parser.add_argument("--setMetaLogs", required=True)
parser.add_argument("--preemptible", action="store_true", default=False, required=False)
args = parser.parse_args()

# Build "Incoming Data" Pipeline

configPath = os.path.join(os.environ["HOME"], ".isb-cgc-pipelines", "config")

pipelinesConfig = PipelinesConfig(configPath)

pipelineBuilder = PipelineBuilder(pipelinesConfig)

setMetaFiles = []

files = DataUtils.getAnalysisDetail(args.analysisId)["result_set"]["results"][0]["files"]

objectPath = DataUtils.constructObjectPath(args.analysisId, args.cghubOutput)

cghubSchema = PipelineSchema("cghub", pipelinesConfig, args.cghubLogs, "b.gcr.io/isb-cgc-public-docker-images/gtdownload",
                             scriptUrl=args.cghubScript,
                             cores=4,
                             diskSize=DataUtils.calculateDiskSize(analysisId=args.analysisId, roundToNearestGbInterval=100),
                             diskType="PERSISTENT_SSD",
                             env="ANALYSIS_ID={analysisId},CGHUB_KEY={key}".format(analysisId=args.analysisId, key=os.path.basename(args.cghubKey)),
                             inputs="{cghubKey}:/{cghubKeyFileName}".format(cghubKey=args.cghubKey, cghubKeyFileName=os.path.basename(args.cghubKey)),
                             outputs="{analysisId}/*:{outputPath}".format(analysisId=args.analysisId, outputPath=objectPath),
                             tag=args.analysisId,
                             preemptible=True)

pipelineBuilder.addStep(cghubSchema)

## DOWNSTREAM STEPS ##

setMetaParents = []

foundBai = False
foundBam = False
foundFastq = False


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



# FASTQC STEP
fastqcOutput = DataUtils.constructObjectPath(args.analysisId, args.fastqcOutput)
fastqcSchema = PipelineSchema("fastqc", pipelinesConfig, args.fastqcLogs, "b.gcr.io/isb-cgc-public-docker-images/fastqc",
                              scriptUrl="gs://isb-cgc-data-02-misc/pipeline-scripts/fastqc.sh",
                              diskSize=DataUtils.calculateDiskSize(analysisId=args.analysisId, roundToNearestGbInterval=100),
                              diskType="PERSISTENT_SSD",
                              inputs=",".join([os.path.join(objectPath, x["filename"]) + ":" + bamFileName for x in files]),
                              outputs="*_fastqc.zip:{fastqcOutput},*_fastqc.html:{fastqcOutput}".format(fastqcOutput=fastqcOutput),
                              env="INPUT_FILENAME={bamFile},OUTPUT_PREFIX={analysisId}".format(bamFile=bamFileName, analysisId=args.analysisId),
                              tag=args.analysisId,
                              preemptible=True)

setMetaFiles.append(os.path.join(args.fastqcOutput, objectPath, "{analysisId}*".format(analysisId=args.analysisId))) # will glob patterns work with `gsutil setmeta ...`?
pipelineBuilder.addStep(fastqcSchema)
cghubSchema.addChild(fastqcSchema)
setMetaParents.append(fastqcSchema)

if foundBam:
	if not foundBai:
		baiFileName = "{filename}.bai".format(filename=bamFileName)
		samtoolsIndexSchema = PipelineSchema("samtools-index", pipelinesConfig, args.samtoolsIndexLogs, "b.gcr.io/isb-cgc-public-docker-images/samtools:1.3.1",
		                                     cmd="samtools index {filename}".format(filename=bamFileName),
		                                     diskSize=DataUtils.calculateDiskSize(analysisId=args.analysisId, roundToNearestGbInterval=100),
		                                     diskType="PERSISTENT_SSD",
		                                     inputs=",".join([os.path.join(objectPath, x["filename"]) + ":" + bamFileName for x in files]),
		                                     outputs="{filename}.bai:{outputPath}".format(filename=bamFileName, outputPath=objectPath),
		                                     tag=args.analysisId,
		                                     preemptible=True)

		pipelineBuilder.addStep(samtoolsIndexSchema)
		cghubSchema.addChild(samtoolsIndexSchema)
		samtoolsIndexSchema.addChild(fastqcSchema)

		setMetaParents.append(samtoolsIndexSchema)

	setMetaFiles.append(os.path.join(args.cghubOutput, objectPath, baiFileName))

# SETMETA STEP

setMetaSchema = PipelineSchema('setmeta', pipelinesConfig, args.setMetaLogs, "google/cloud-sdk",
                               cmd='gsutil setmeta -h \"x-goog-meta-analysis-id:{analysisId}\" {setMetaFiles}'.format(analysisId=args.analysisId, setMetaFiles=' '.join(setMetaFiles)),
                               diskSize=10,
                               tag=args.analysisId,
                               preemptible=True)

for p in setMetaParents:
	p.addChild(setMetaSchema)

pipelineBuilder.addStep(setMetaSchema)

pipelineBuilder.run()	


