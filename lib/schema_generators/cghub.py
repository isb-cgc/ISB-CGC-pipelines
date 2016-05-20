import os
import uuid
import argparse
from ..pipelines.utils import DataUtils
from ..pipelines.schema import PipelineSchema


def generate(args, config):
	parser = argparse.ArgumentParser()
	parser.add_argument("--analysisId", required=True)
	parser.add_argument("--cghubKey", required=False)
	parser.add_argument("--diskSize", required=True)
	parser.add_argument("--outputPath", required=True)
	parser.add_argument("--logsPath", required=True)
	parser.add_argument("--preemptible", action="store_true", required=False, default=False)
	args = parser.parse_args(args=args)

	cghubSchema = PipelineSchema('cghub', args.analysisId, config)

	# get information from cghub about the given analysis id
	analysisDetail = DataUtils.getAnalysisDetail(args.analysisId)
	objectPath = DataUtils.constructObjectPath(args.analysisId, args.outputPath)

	if len(analysisDetail["result_set"]["results"]) > 0:
		files = analysisDetail["result_set"]["results"][0]["files"]

		cghubDiskName = "cghub-{uuid}".format(uuid=str(uuid.uuid4()).split('-')[0])
		cghubDiskType = "PERSISTENT_SSD"
		cghubDiskMountPath = "/cghub"
		cghubSchema.addDisk(cghubDiskName, cghubDiskType, args.diskSize, cghubDiskMountPath)

		cghubSchema.addInput("cghubKey", cghubDiskName, os.path.basename(args.cghubKey), args.cghubKey)
		cghubSchema.addOutput("downloadedFiles", cghubDiskName, "{analysisId}/*".format(analysisId=args.analysisId), os.path.join(args.outputPath, objectPath))

		if args.cghubKey:
			cghubSchema.setCmd((
				'cd {diskMountPath}; '
				'max_dl_attempts=3; '
				'while [ \"$max_dl_attempts\" -ge 0 ]; '
				'do if [ \"$max_dl_attempts\" -eq 0 ]; '
				'then exit -1; '
				'else gtdownload -v --max-children 4 -k 60 -c {cghubKeyFileName} -d {analysisId} --path .; '
				'if [ \"$?\" -ne 0 ]; '
				'then sleep 10; '
				'max_dl_attempts=$(expr $max_dl_attempts - 1); '
				'else break; '
				'fi; '
				'fi; '
				'done'
			).format(diskMountPath=cghubDiskMountPath, cghubKeyFileName=os.path.basename(args.cghubKey), analysisId=args.analysisId))
		else:
			cghubSchema.setCmd((
				'cd {diskMountPath}; '
				'max_dl_attempts=3; '
				'while [ \"$max_dl_attempts\" -ge 0 ]; '
				'do if [ \"$max_dl_attempts\" -eq 0 ]; '
				'then exit -1; '
				'else gtdownload -v --max-children 4 -k 60 -d {analysisId} --path .; '
				'if [ \"$?\" -ne 0 ]; '
				'then sleep 10; '
				'max_dl_attempts=$(expr $max_dl_attempts - 1); '
				'else break; '
				'fi; '
				'fi; '
				'done'
			).format(diskMountPath=cghubDiskMountPath, analysisId=args.analysisId))

		cghubSchema.setImage("b.gcr.io/isb-cgc-public-docker-images/gtdownload")
		cghubSchema.setMem(4)
		cghubSchema.setCpu(4)

		cghubSchema.setLogOutput(args.logsPath)

		cghubSchema.addSchemaMetadata(files=files, objectPath=objectPath)

		if args.preemptible:
			cghubSchema.setPreemptible(True)

		return cghubSchema
	else:
		raise LookupError("Analysis ID {a} returned no results!".format(a=args.analysisId))
