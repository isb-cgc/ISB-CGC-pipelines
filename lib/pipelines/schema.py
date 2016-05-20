class PipelineSchema(object):
	def __init__(self, name, tag, config):  # config must be an instance of pipelines.utils.PipelinesConfig
		self.name = name
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

