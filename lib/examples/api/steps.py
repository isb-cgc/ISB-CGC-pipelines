import os
import re
import csv
import argparse
import subprocess
from pipelines.utils import PipelinesConfig, DataUtils, PipelineSchema, PipelineBuilder
from pipelines.paths import CLIENT_CONFIG_PATH

class PipelineStep(object):
	def __init__(self):
		self._pipelinesConfig = PipelinesConfig(CLIENT_CONFIG_PATH)
		self._step = None

	def get(self):
		return self._step

class FastQCStep(PipelineStep):
	def __init__(self, inputUrl, outputUrl, outputPrefix, diskSize, diskType, logsPath, container, scriptUrl, tag, cores, mem, preemptible):
		super(PipelineStep, self).__init__()

		inputFileName = os.path.basename(inputUrl)

		self._step = PipelineSchema("fastqc",
		                            self._pipelinesConfig,
		                            logsPath,
		                            container,
		                            scriptUrl=scriptUrl,
		                            diskSize=diskSize,
		                            diskType=diskType,
		                            mem=mem,
		                            cores=cores,
		                            inputs="{inputUrl}:{inputFileName}".format(inputUrl=inputUrl, inputFileName=inputFileName),
		                            outputs="*_fastqc.zip:{fastqcOutput},*_fastqc.html:{fastqcOutput}".format(fastqcOutput=outputUrl),
		                            env="INPUT_FILENAME={inputFileName},OUTPUT_PREFIX={outputPrefix}".format(inputFileName=inputFileName, outputPrefix=outputPrefix),
		                            tag=tag,
		                            preemptible=preemptible)


class SamtoolsIndexStep(PipelineStep):
	def __init__(self, bamUrl, outputUrl, diskSize, diskType, logsPath, container, scriptUrl, tag, cores, mem, preemptible):
		super(PipelineStep, self).__init__()

		bamFileName = os.path.basename(bamUrl)

		self._step = PipelineSchema("samtoolsIndex",
		                            self._pipelinesConfig,
		                            logsPath,
		                            container,
		                            scriptUrl=scriptUrl,
		                            diskSize=diskSize,
		                            diskType=diskType,
		                            mem=mem,
		                            cores=cores,
		                            inputs="{bamUrl}:{bamFileName}".format(bamUrl=bamUrl, bamFileName=bamFileName),
		                            outputs="*.bai:{outputUrl}".format(outputUrl=outputUrl),
		                            env="BAM_FILENAME={bamFileName}".format(bamFileName=bamFileName),
		                            tag=tag,
		                            preemptible=preemptible)


class SamtoolsFaidxStep(PipelineStep):
	def __init__(self, fastaUrl, outputUrl, diskSize, diskType, logsPath, container, scriptUrl, tag, cores, mem, preemptible):
		super(PipelineStep, self).__init__()

		fastaFileName = os.path.basename(fastaUrl)

		self._step = PipelineSchema("samtools-faidx",
		                            self._pipelinesConfig,
		                            logsPath,
									container,
		                            scriptUrl=scriptUrl,
		                            diskSize=diskSize,
		                            diskType=diskType,
		                            cores=cores,
		                            mem=mem,
		                            tag=tag,
		                            inputs="{fastaUrl}:{fastaFileName}".format(fastaUrl=fastaUrl, fastaFileName=fastaFileName),
		                            outputs="*.fai:{outputUrl}".format(outputUrl=outputUrl),
		                            preemptible=preemptible)

class BiobloommakerStep(PipelineStep):
	def __init__(self, fastaDirUrl, outputPrefix, outputUrl, diskSize, diskType, logsPath, container, scriptUrl, tag, cores, mem, preemptible):
		super(PipelineStep, self).__init__()

		try:
			fastaDirContents = subprocess.check_output(["gsutil", "ls", fastaDirUrl])

		except subprocess.CalledProcessError as e:
			print "ERROR: couldn't get a listing of reference files!  -- {reason}".format(reason=e)
			exit(-1)

		fastaInputs = [x for x in fastaDirContents.split('\n') if re.match('^.*\.fa$', x) or re.match('^.*\.fai$', x) or re.match('^.*\.fasta$', x)]
		inputs = ','.join(["{url}:{filename}".format(url=x, filename=os.path.basename(x)) for x in fastaInputs])
		outputs = "*.bf:{filterOutput},*.txt:{filterOutput}".format(filterOutput=outputUrl)
		env = "OUTPUT_PREFIX={outputPrefix},VIRAL_SEQUENCES={viralSequences}".format(outputPrefix=outputPrefix, viralSequences=' '.join(fastaInputs))

		self._step = PipelineSchema("biobloommaker",
		                            self._pipelinesConfig,
		                            logsPath,
		                            container,
		                            scriptUrl=scriptUrl,
		                            cores=cores,
		                            mem=mem,
		                            tag=tag,
		                            diskSize=diskSize,
		                            diskType=diskType,
		                            inputs=inputs,
		                            outputs=outputs,
		                            env=env,
		                            preemptible=preemptible)

class BiobloomcategorizerStep(PipelineStep):
	def __init__(self, fqArchiveUrl, filtersDir, outputPrefix, outputUrl, diskSize, diskType, logsPath, container, scriptUrl, tag, cores, mem, preemptible):
		super(PipelineStep, self).__init__()

		fqFileName = os.path.basename(fqArchiveUrl)
		fqInputs = "{fqArchive}:{fqFileName}".format(fqArchive=fqArchiveUrl, fqFileName=fqFileName)

		try:
			filtersDirContents = subprocess.check_output(["gsutil", "ls", filtersDir])

		except subprocess.CalledProcessError as e:
			print "ERROR: couldn't get a listing of filter files!  -- {reason}".format(reason=e)
			exit(-1)

		bfInputs = [x for x in filtersDirContents.split('\n') if re.match('^.*\.bf$', x) or re.match('^.*\.txt', x)]
		bfInputs.append(fqInputs)

		inputs = ",".join(["{url}:{filename}".format(url=x, filename=os.path.basename(x)) for x in bfInputs])
		outputs = "{outputPrefix}*:{outDir}".format(outputPrefix=outputPrefix, outDir=outputUrl)
		env = "INPUT_FILE={fqFileName},OUTPUT_PREFIX={outputPrefix},FILTERS_LIST={filtersList}".format(fqFileName=fqFileName, outputPrefix=outputPrefix, filtersList=','.join([os.path.basename(x) for x in bfInputs if re.match('^.*\.bf$', x)]))

		self._step = PipelineSchema("biobloomcategorizer",
		                                           self._pipelinesConfig,
		                                           logsPath,
		                                           container,
		                                           scriptUrl=scriptUrl,
		                                           cores=cores,
		                                           mem=mem,
		                                           diskSize=diskSize,
		                                           diskType=diskType,
		                                           inputs=inputs,
		                                           outputs=outputs,
		                                           env=env,
		                                           tag=tag,
		                                           preemptible=preemptible)

class GDCDownloadStep(PipelineStep):
	def __init__(self, fileUuid, tokenUrl, outputUrl, diskSize, diskType, logsPath, container, scriptUrl, tag, cores, mem, preemptible):
		super(PipelineStep, self).__init__()

		tokenFileName = os.path.basename(tokenUrl)

		self._step = PipelineSchema("gdcDownload",
		                            self._pipelinesConfig,
		                            logsPath,
		                            container,
		                            scriptUrl=scriptUrl,
		                            cores=cores,
		                            mem=mem,
		                            diskSize=diskSize,
		                            diskType=diskType,
		                            inputs="{tokenUrl}:{tokenFileName}".format(tokenUrl=tokenUrl, tokenFileName=tokenFileName),
		                            outputs="{fileUuid}/*:{outputUrl}".format(fileUuid=fileUuid, outputUrl=outputUrl),
		                            env="FILE_UUID={fileUuid},GDC_TOKEN={tokenFileName}".format(fileUuid=fileUuid, tokenFileName=tokenFileName),
		                            tag=tag,
		                            preemptible=preemptible)

class MitcrStep(PipelineStep):
	def __init__(self, fqArchiveUrl, psetFileUrl, outputFileName, outputUrl, diskSize, diskType, logsPath, container, scriptUrl, tag, cores, mem, preemptible):
		super(PipelineStep, self).__init__()

		fqFileName = os.path.basename(fqArchiveUrl)
		psetFileName = os.path.basename(psetFileUrl)

		self._step = PipelineSchema("mitcr",
		                            self._pipelinesConfig,
		                            logsPath,
		                            container,
		                            scriptUrl=scriptUrl,
		                            cores=cores,
		                            mem=mem,
		                            diskSize=diskSize,
		                            diskType=diskType,
		                            inputs="{fqArchiveUrl}:{fqFileName},{psetFileUrl}:{psetFileName}".format(fqArchiveUrl=fqArchiveUrl, fqFileName=fqFileName, psetFileUrl=psetFileUrl, psetFileName=psetFileName),
		                            outputs="{outputFileName}:{outputUrl}".format(outputFileName=outputFileName, outputUrl=outputUrl),
		                            env="INPUT_FILE={fqFileName},PSET_FILE={psetFileName},OUTPUT_FILE={outputFileName}".format(fqFileName=fqFileName, psetFileName=psetFileName, outputFileName=outputFileName),
		                            tag=tag,
		                            preemptible=preemptible)

class PicardStep(PipelineStep):
	def __init__(self, bamUrl, outputPrefix, outputUrl, diskSize, diskType, logsPath, container, scriptUrl, tag, cores, mem, preemptible):
		super(PipelineStep, self).__init__()

		bamFileName = os.path.basename(bamUrl)

		self._step = PipelineSchema("picard",
		                            self._pipelinesConfig,
		                            logsPath,
		                            container,
		                            scriptUrl=scriptUrl,
		                            cores=cores,
		                            mem=mem,
		                            diskSize=diskSize,
		                            diskType=diskType,
		                            inputs="{bamUrl}:{bamFileName},{bamUrl}.bai:{bamFileName}.bai".format(bamUrl=bamUrl, bamFileName=bamFileName),
		                            outputs="{outputPrefix}*:{outputUrl}".format(outputPrefix=outputPrefix, outputUrl=outputUrl),
		                            env="INPUT_FILENAME={bamFileName},OUTPUT_PREFIX={outputPrefix}".format(bamFileName=bamFileName, outputPrefix=outputPrefix),
		                            tag=tag,
		                            preemptible=preemptible)
