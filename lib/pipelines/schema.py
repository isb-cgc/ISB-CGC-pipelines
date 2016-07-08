import os
import uuid


class PipelineSchema(object):
	def __init__(self, name, config, logsPath, imageName, scriptUrl=None, cmd=None, cores=1, mem=1, diskSize=None,
	             diskType=None, env=None, inputs=None, outputs=None, tag=None, children=None, metadata=None,
	             preemptible=False):  # config must be an instance of pipelines.config.PipelineConfig
		self.name = name

		if tag is None:
			tag = str(uuid.uuid4())

		self.tag = tag
		self._schema = {
			"name": name,
			"tag": tag,
			"children": [],
			"request": {
				"pipelineArgs": {
					"projectId": config.project_id,
					"resources": {
						"disks": [],
						"zones": config.zones.split(',')
					},
					"logging": {},
					"inputs": {},
					"outputs": {},
					"serviceAccount": {
						"email": config.service_account_email,
						"scopes": config.scopes.split(',')
					}
				},
				"ephemeralPipeline": {
					"name": name,
					"projectId": config.project_id,
					"inputParameters": [],
					"outputParameters": [],
					"resources": {
						"disks": [],
						"zones": config.zones.split(',')
					},
					"docker": {}
				}
			}
		}
		self._metadata = {}

		# if disk info provided, add a disk
		if diskSize is not None:
			mountPath = "/{pipeline}".format(pipeline=name)
			if diskType is None:
				diskType = "PERSISTENT_SSD"

			self.addDisk(name, diskType, diskSize, mountPath)

		# add inputs
		# TODO: input validation
		if inputs is not None:
			inputMap = {':'.join(pair.split(':')[0:-1]): pair.split(':')[-1] for pair in inputs.split(',')}

			for i, k in enumerate(inputMap.keys()):
				inputName = "input{N}".format(N=i)
				self.addInput(inputName, name, inputMap[k], k)

		# add outputs
		# TODO: input validation
		if outputs is not None:
			outputMap = {pair.split(':')[0]: ':'.join(pair.split(':')[1:]) for pair in outputs.split(',')}

			for i, k in enumerate(outputMap.keys()):
				outputName = "output{N}".format(N=i)
				self.addOutput(outputName, name, k, outputMap[k])

		# set log output path
		self.setLogOutput(logsPath)

		# set resources
		self.setCpu(cores)
		self.setMem(mem)

		# prepare environment string
		# TODO: set some default config variables in the envString (i.e., JOBNAME)
		if env is not None:
			envString = " ".join(env.split(',')) + " "
		else:
			envString = ""

		# set docker
		self.setImage(imageName)
		if scriptUrl is not None:
			script = os.path.basename(scriptUrl)
			self.addInput("pipelineScript", name, script, scriptUrl)
			command = (
				'cd {mnt} && ls && '
				'chmod u+x {script} && '
				'{env}./{script}'
			).format(mnt=mountPath, script=script, env=envString)
		elif cmd is not None:
			command = (
				'cd {mnt} && '
				'{env}{cmd}'
			).format(mnt=mountPath, env=envString, cmd=cmd)

		self.setCmd(command)

		# set preemptibility
		self.setPreemptible(preemptible)

		# set metadata
		if metadata is not None:
			metadataMap = {pair.split('=')[0]: pair.split('=')[1].split(',') for pair in metadata.split(',')}
			self.addSchemaMetadata(**metadataMap)

		# set children
		if children is not None:
			childList = ','.split(children)

			for c in childList:
				self.addChild(c)

	def getSchema(self):
		return self._schema

	def addChild(self, child):
		self._schema["children"].append(child.name)

	def addInput(self, name, disk, localPath, gcsPath):
		self._schema["request"]["pipelineArgs"]["inputs"][name] = gcsPath

		self._schema["request"]["ephemeralPipeline"]["inputParameters"].append({
			"name": name,
			"localCopy": {
				"path": localPath,
				"disk": disk
			}
		})

	def addOutput(self, name, disk, localPath, gcsPath):
		self._schema["request"]["pipelineArgs"]["outputs"][name] = gcsPath

		self._schema["request"]["ephemeralPipeline"]["outputParameters"].append({
			"name": name,
			"localCopy": {
				"path": localPath,
				"disk": disk
			}
		})

	def addDisk(self, name, diskType, sizeGb, mountPath, autoDelete=True, readOnly=False):
		self._schema["request"]["pipelineArgs"]["resources"]["disks"].append({
			"name": name,
			"type": diskType,
			"sizeGb": sizeGb,
			"autoDelete": autoDelete,
			"readOnly": readOnly
		})
		self._schema["request"]["ephemeralPipeline"]["resources"]["disks"].append({
			"name": name,
			"type": diskType,
			"sizeGb": sizeGb,
			"autoDelete": autoDelete,
			"readOnly": readOnly,
			"mountPoint": mountPath
		})

	def setLogOutput(self, gcsPath):
		self._schema["request"]["pipelineArgs"]["logging"]["gcsPath"] = gcsPath

	def setMem(self, memGb):
		self._schema["request"]["pipelineArgs"]["resources"]["minimumRamGb"] = memGb
		self._schema["request"]["ephemeralPipeline"]["resources"]["minimumRamGb"] = memGb

	def setCpu(self, cores):
		self._schema["request"]["pipelineArgs"]["resources"]["minimumCpuCores"] = cores
		self._schema["request"]["ephemeralPipeline"]["resources"]["minimumCpuCores"] = cores

	def setBootDiskSize(self, sizeGb):
		self._schema["request"]["pipelineArgs"]["resources"]["bootDiskSizeGb"] = sizeGb
		self._schema["request"]["ephemeralPipeline"]["resources"]["bootDiskSizeGb"] = sizeGb

	def setPreemptible(self, preemptible):
		self._schema["request"]["pipelineArgs"]["resources"]["preemptible"] = preemptible
		self._schema["request"]["ephemeralPipeline"]["resources"]["preemptible"] = preemptible

	def setImage(self, imageName):
		self._schema["request"]["ephemeralPipeline"]["docker"]["imageName"] = imageName

	def setCmd(self, cmdString):
		self._schema["request"]["ephemeralPipeline"]["docker"]["cmd"] = cmdString

	def addSchemaMetadata(self, **kwargs):
		self._metadata.update(**kwargs)

	def getSchemaMetadata(self, key):
		if key in self._metadata.keys():
			return self._metadata[key]
		else:
			raise LookupError("Metadata key '{k}' not found".format(k=key))