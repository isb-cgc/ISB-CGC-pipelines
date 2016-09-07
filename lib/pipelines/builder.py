import json
from jsonspec.validators import load, ValidationError  # jsonspec is licensed under BSD
from pipelines.db import PipelineDatabase, PipelineDatabaseError
from pipelines.queue import PipelineQueue, PipelineQueueError


class PipelineSchemaValidationError(Exception):  # TODO: implement
	def __init__(self, msg):
		super(PipelineSchemaValidationError, self).__init__()
		self.msg = msg

class PipelineSubmitError(Exception):
	def __init__(self, msg):
		super(PipelineSubmitError, self).__init__()
		self.msg = msg


class PipelineBuilder(object):
	def __init__(self, config):
		self._pipelines = []
		self._schema = {}
		self._config = config
		self._dependencyMap = {}
		self._pipelineDatabase = PipelineDatabase(self._config)
		self._pipelineQueue = PipelineQueue('WAIT_Q')

	def addStep(self, root):  # root must be an instance of PipelineSchema
		if self.hasStep(root.name):
			raise ValueError("Pipeline already contains a step with name {n}".format(n=root.name))

		self._dependencyMap[root.name] = root.getSchema()

		self._pipelines.append(self._dependencyMap[root.name])

	def hasStep(self, stepName):
		hasStep = False
		for p in self._pipelines:
			if p == stepName:
				hasStep = True

		return hasStep

	def run(self):
		# generate schema
		self._generateSchema()

		# schema validation
		self._validateSchema()

		# initialize job configs
		jobIds = self._submitSchema()
		return jobIds

	def _generateSchema(self):
		self._schema.update({"pipelines": self._pipelines})

	def _validateSchema(self):  # TODO: add fields for default values for inputs if there are no parents
		try:
			jobs = self._schema["pipelines"]
		except KeyError as e:
			raise PipelineSchemaValidationError("There was a problem getting the list of pipelines from the specification: {reason}".format(reason=e))

		jobNames = []
		for job in jobs:
			jobNames.append(job["name"])

		if len(jobNames) != len(set(jobNames)):
			raise PipelineSchemaValidationError("Pipeline names must be unique")

		for job in jobs:
			if "children" in job.keys() and "parents" not in job.keys():
				for child in job["children"]:
					if child not in jobNames:
						raise PipelineSchemaValidationError("job '{jobName}' specifies a child that doesn't exist".format(jobName=job["name"]))

		pipelineSchema = {
			"description": "Pipeline Graph Schema",
			"type": "object",
			"properties": {
				"pipelines": {
					"type": "array",
					"items": {
						"type": "object",
						"additionalProperties": {"$ref": "#/definitions/pipeline"},
					}
				}
			},
			"definitions": {
				"pipeline": {
					"name": {
						"description": "The name of the pipeline to run on an input file",
						"type": "string",
						"required": True
					},
					"tag": {
						"description": "An arbitrary identifier for the pipeline",
						"type": "string",
						"required": True
					},
					"children": {
						"description": "The names of the child pipelines, if there are any -- must exist in the 'pipelines' array",
						"type": "array",
						"items": {
							"type": "string"
						},
						"required": False
					},
					"request": {
						"description": "The Google Genomics Pipelines API request object",
						"type": "object"  # TODO: schema validation for the request object
					}
				}
			}
		}

		validator = load(pipelineSchema)
		validator.validate(self._schema)
		try:
			validator.validate(self._schema)
		except ValidationError as e:  # what kind of exception?
			raise PipelineSchemaValidationError("Couldn't validate the pipeline schema: {reason}".format(reason=e))

	def _submitSchema(self):
		jobIdMap = {}

		for p in self._schema["pipelines"]:  # Add all jobs to the jobs table
			try:
				jobIdMap[p["name"]] = self._pipelineDatabase.insertJob(None, None, p["name"], p["tag"], None, 0,
			                                                      p["request"]["pipelineArgs"]["logging"]["gcsPath"],
			                                                      None, None, None, None, None,
			                                                      json.dumps(p["request"]))
			except PipelineDatabaseError as e:
				raise PipelineSubmitError("Couldn't create pipeline database record: {reason}".format(reason=e))

		for p in self._schema["pipelines"]:  # Add dependency info to the job dependency table
			if "children" in p.keys() and len(p["children"]) > 0:
				for c in p["children"]:
					parentId = jobIdMap[p["name"]]
					childId = jobIdMap[c]

					try:
						self._pipelineDatabase.insertJobDependency(parentId, childId)
					except PipelineDatabaseError as e:
						raise PipelineSubmitError("Couldn't update job dependencies: {reason}".format(reason=e))

		for p in self._schema["pipelines"]:  # schedule pipelines
			try:
				parents = self._pipelineDatabase.getParentJobs(jobIdMap[p["name"]])
			except PipelineDatabaseError as e:
				raise PipelineSubmitError("Couldn't get job dependency list: {reason}".format(reason=e))

			try:
				self._pipelineDatabase.updateJob(jobIdMap[p["name"]], setValues={"current_status": "WAITING"},
			                                keyName="job_id")
			except PipelineDatabaseError as e:
				raise PipelineSubmitError("Couldn't update job dependencies: {reason}".format(reason=e))

			# if the job is a root job, send the job request to the queue
			msg = {
				"job_id": jobIdMap[p["name"]],
				"request": p["request"]
			}

			if len(parents) == 0:
				try:
					self._pipelineQueue.publish(json.dumps(msg))
				except PipelineQueueError as e:
					raise PipelineSubmitError("Couldn't submit job to queue: {reason}".format(reason=e))

		return jobIdMap.itervalues()
