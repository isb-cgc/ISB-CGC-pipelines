import os
import sys
import uuid
import pika
import math
import json
import sqlite3
import requests
import argparse
import pyinotify
import subprocess
from time import time
from tempfile import mkstemp
from datetime import datetime
from jsonspec.validators import load  # jsonspec is licensed under BSD
from ConfigParser import SafeConfigParser, NoOptionError, NoSectionError

from googleapiclient.errors import HttpError

import pprint

MODULE_PATH = "/usr/local/ISB-CGC-pipelines/lib"  # TODO: move path to configuration file


class PipelineSchema(object):
	def __init__(self, name, config, logsPath, imageName, scriptUrl=None, cmd=None, cores=1, mem=1, diskSize=None,
	             diskType=None, env=None, inputs=None, outputs=None, tag=None, children=None, metadata=None,
	             preemptible=False):  # config must be an instance of pipelines.utils.PipelinesConfig
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

		defaultMountPath = "/{pipeline}".format(pipeline=name)

		# if disk info provided, add a disk
		if diskSize is not None:
			if diskType is None:
				diskType = "PERSISTENT_SSD"

			self.addDisk(name=name, diskType=diskType, diskSize=diskSize, mountPath=defaultMountPath)

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
		# TODO: implement a "setEnv" method which takes arbitrary kwargs
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
			).format(mnt=defaultMountPath, script=script, env=envString)
		elif cmd is not None:
			command = (
				'cd {mnt} && '
				'{env}{cmd}'
			).format(mnt=defaultMountPath, env=envString, cmd=cmd)

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

	def addInput(self, name=None, disk=None, localPath=None, gcsPath=None):
		self._schema["request"]["pipelineArgs"]["inputs"][name] = gcsPath

		self._schema["request"]["ephemeralPipeline"]["inputParameters"].append({
			"name": name,
			"localCopy": {
				"path": localPath,
				"disk": disk
			}
		})

	def addOutput(self, name=None, disk=None, localPath=None, gcsPath=None):
		self._schema["request"]["pipelineArgs"]["outputs"][name] = gcsPath

		self._schema["request"]["ephemeralPipeline"]["outputParameters"].append({
			"name": name,
			"localCopy": {
				"path": localPath,
				"disk": disk
			}
		})

	def addDisk(self, name=None, diskType=None, sizeGb=None, mountPath=None, autoDelete=True, readOnly=False):
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


class PipelineBuilder(object):
	def __init__(self, config):
		self._pipelines = []
		self._schema = {}
		self._config = config
		self._dependencyMap = {}
		self._pipelineDbUtils = PipelineDbUtils(self._config)
		self._pipelineQueueUtils = PipelineQueueUtils('WAIT_Q')

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
		self._submitSchema()

	def _generateSchema(self):
		self._schema.update({"pipelines": self._pipelines})

	def _validateSchema(self):  # TODO: add fields for default values for inputs if there are no parents
		try:
			jobs = self._schema["pipelines"]
		except KeyError as e:
			print "There was a problem getting the list of pipelines from the specification"
			exit(-1)

		jobNames = []
		for job in jobs:
			jobNames.append(job["name"])

		if len(jobNames) != len(set(jobNames)):
			print "ERROR: pipeline names must be unique"
			exit(-1)

		for job in jobs:
			if "children" in job.keys() and "parents" not in job.keys():
				for child in job["children"]:
					if child not in jobNames:
						print "ERROR: job '{jobName}' specifies a child that doesn't exist".format(jobName=job["name"])
						exit(-1)

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
		except:  # what kind of exception?
			exit(-1)

	def _submitSchema(self):
		jobIdMap = {}

		for p in self._schema["pipelines"]:  # Add all jobs to the jobs table
			jobIdMap[p["name"]] = self._pipelineDbUtils.insertJob(None, None, p["name"], p["tag"], None, 0,
			                                                      p["request"]["pipelineArgs"]["logging"]["gcsPath"],
			                                                      None, None, None, None, None,
			                                                      json.dumps(p["request"]))

		for p in self._schema["pipelines"]:  # Add dependency info to the job dependency table
			if "children" in p.keys() and len(p["children"]) > 0:
				for c in p["children"]:
					parentId = jobIdMap[p["name"]]
					childId = jobIdMap[c]
					self._pipelineDbUtils.insertJobDependency(parentId, childId)

		for p in self._schema["pipelines"]:  # schedule pipelines
			parents = self._pipelineDbUtils.getParentJobs(jobIdMap[p["name"]])
			self._pipelineDbUtils.updateJob(jobIdMap[p["name"]], setValues={"current_status": "WAITING"},
			                                keyName="job_id")

			# if the job is a root job, send the job request to the queue
			msg = {
				"job_id": jobIdMap[p["name"]],
				"request": p["request"]
			}

			pprint.pprint(msg)

			if len(parents) == 0:
				self._pipelineQueueUtils.publish(json.dumps(msg))


class PipelinesConfig(SafeConfigParser, object):
	def __init__(self, path, first_time=False):
		super(PipelinesConfig, self).__init__()

		self.path = path

		try:
			os.makedirs(os.path.dirname(self.path))
		except OSError:
			pass

		self._configParams = {
			"project_id": {
				"section": "gcp",
				"required": True,
				"default": None,
				"message": "Enter your GCP project ID: "
			},
			"zones": {
				"section": "gcp",
				"required": True,
				"default": "us-central1-a,us-central1-b,us-central1-c,us-central1-f,us-east1-b,us-east1-c,us-east1-d",
				"message": "Enter a comma-delimited list of GCE zones (leave blank to use the default list of all US zones): "
			},
			"scopes": {
				"section": "gcp",
				"required": True,
				"default": "https://www.googleapis.com/auth/genomics,https://www.googleapis.com/auth/compute,https://www.googleapis.com/auth/devstorage.full_control",
				"message": "Enter a comma-delimited list of GCP scopes (leave blank to use the default list of scopes): "
			},
			"service_account_email": {
				"section": "gcp",
				"required": True,
				"default": "default",
				"message": "Enter a valid service account email (leave blank to use the default service account): "
			},
			"pipelines_home": {  # TODO: get rid of this parameter (no longer need it)
				"section": "pipelines",
				"required": True,
				"default": os.path.join(os.path.dirname(self.path), "pipelines"),
				"message": "Enter a path for the ISB-CGC Pipelines job directory (leave blank to use ~/.isb-cgc-pipelines/pipelines by default): "
			},
			"max_running_jobs": {
				"section": "pipelines",
				"required": True,
				"default": 2000,
				"message": "Enter the maximum number of running jobs for any given time (leave blank to use default 2000): "
			},
			"autorestart_preempted": {
				"section": "pipelines",
				"required": True,
				"default": False,
				"message": "Would you like to automatically restart preempted jobs? (Only relevant when submitting jobs with the '--preemptible' flag; default is No) Y/N : "
			},
			"db": {
				"section": "pipelines",
				"required": True,
				"default": "sqlite",
				"message": "Enter the type of database to use (leave blank to use the default 'sqlite'): "
			}
		}

		if first_time:
			for option, attrs in self._configParams.iteritems():
				if not self.has_section(attrs["section"]):
					self.add_section(attrs["section"])

				if attrs["required"]:
					val = raw_input(attrs["message"])
					if len(val) == 0:
						self.update(attrs["section"], option, attrs["default"], first_time=True)
					else:
						self.update(attrs["section"], option, val, first_time=True)

		self.refresh()

	def update(self, section, option, value, first_time=False):
		if option not in self._configParams.keys():
			raise ValueError("unrecognized option {s}/{o}".format(s=section, o=option))
		else:
			if self._configParams[option]["section"] != section:
				raise ValueError("unrecognized section {s}".format(s=section))

		if not self.has_section(section):
			self.add_section(section)

		self.set(section, str(option), str(value))

		with open(self.path, 'w') as f:
			self.write(f)

		if not first_time:
			self.refresh()

	def watch(self):
		# watch changes to the config file -- needs to be run in a separate thread
		configStatusManager = pyinotify.WatchManager()
		configStatusNotifier = pyinotify.Notifier(configStatusManager)
		configStatusManager.add_watch(self.path, pyinotify.IN_CLOSE_WRITE, proc_fun=PipelinesConfigUpdateHandler(config=self))
		configStatusNotifier.loop()

	def refresh(self):
		self.__dict__.update(self._verify())

	def _verify(self):
		try:
			self.read(self.path)
		except IOError as e:
			print "Couldn't open {path}: {reason}".format(path=self.path, reason=e)
			exit(-1)
		else:
			d = {}
			for name, attrs in self._configParams.iteritems():
				if attrs["required"]:
					if not self.has_section(attrs["section"]):
						raise LookupError("missing required section {s} in the configuration!\nRUN `isb-cgc-pipelines config` to correct the configuration".format(s=attrs["section"]))

					if not self.has_option(attrs["section"], name):
						raise LookupError("missing required option {o} in section {s}!\nRun `isb-cgc-pipelines config` to correct the configuration".format(s=attrs["section"], o=name))

				try:
					d[name] = self.get(attrs["section"], name)

				except NoOptionError:
					pass
				except NoSectionError:
					pass

			return d


class PipelinesConfigUpdateHandler(pyinotify.ProcessEvent):
	def my_init(self, config=None):  # config -> PipelinesConfig
		self._config = config

	def process_IN_CLOSE_WRITE(self, event):
		PipelineSchedulerUtils.writeStdout("Refreshing configuration ...")
		self._config.refresh()


class PipelineSchedulerUtils(object):
	@staticmethod
	def startScheduler(config, user):
		# set up the rabbitmq queues
		rabbitmqConn = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
		channel = rabbitmqConn.channel()

		channel.queue_declare(queue='WAIT_Q', durable=True)
		channel.queue_declare(queue='CANCEL_Q', durable=True)
		rabbitmqConn.close()

		pipelineDbUtils = PipelineDbUtils(config)
		pipelineDbUtils.createJobTables()

		c = SafeConfigParser()
		
		try:
			c.readfp(open("/etc/supervisor/supervisord.conf"))
		except IOError as e:
			print "ERROR: supervisor config file (/etc/supervisor/supervisord.conf) -- double check your supervisor installation."
			exit(-1)
		else:
			if not c.has_section("program:pipelineJobScheduler"):
				c.add_section("program:pipelineJobScheduler")

			if not c.has_section("program:pipelineJobCanceller"):
				c.add_section("program:pipelineJobCanceller")

			if not c.has_section("program:pipelinePreemptedLogsHandler"):
				c.add_section("program:pipelinePreemptedLogsHandler")
				
			if not c.has_section("program:pipelineDeleteLogsHandler"):
				c.add_section("program:pipelineDeleteLogsHandler")

			if not c.has_section("program:pipelineInsertLogsHandler"):
				c.add_section("program:pipelineInsertLogsHandler")

			c.set("program:pipelineJobScheduler", "process_name", "pipelineJobScheduler")
			c.set("program:pipelineJobScheduler", "command",
			      "pipelineJobScheduler --config {config}".format(config=config.path))
			c.set("program:pipelineJobScheduler", "environment",
			      "PYTHONPATH={modulePath}".format(modulePath=MODULE_PATH))
			c.set("program:pipelineJobScheduler", "numprocs", "1")
			c.set("program:pipelineJobScheduler", "autostart", "true")
			c.set("program:pipelineJobScheduler", "autorestart", "true")
			c.set("program:pipelineJobScheduler", "user", user)

			c.set("program:pipelineJobCanceller", "process_name", "pipelineJobCanceller")
			c.set("program:pipelineJobCanceller", "command",
			      "pipelineJobCanceller --config {config}".format(config=config.path))
			c.set("program:pipelineJobCanceller", "environment",
			      "PYTHONPATH={modulePath}".format(modulePath=MODULE_PATH))
			c.set("program:pipelineJobCanceller", "numprocs", "1")
			c.set("program:pipelineJobCanceller", "autostart", "true")
			c.set("program:pipelineJobCanceller", "autorestart", "true")
			c.set("program:pipelineJobCanceller", "user", user)

			c.set("program:pipelineInsertLogsHandler", "process_name", "%(program_name)s_%(process_num)s")
			c.set("program:pipelineInsertLogsHandler", "command",
			      "receivePipelineVmLogs --config {config} --subscription pipelineVmInsert".format(config=config.path))
			c.set("program:pipelineInsertLogsHandler", "environment",
			      "PYTHONPATH={modulePath}".format(modulePath=MODULE_PATH))
			c.set("program:pipelineInsertLogsHandler", "numprocs",
			      "10")  # TODO: come up with a formula for determining the number of processes
			c.set("program:pipelineInsertLogsHandler", "autostart", "true")
			c.set("program:pipelineInsertLogsHandler", "autorestart", "true")
			c.set("program:pipelineInsertLogsHandler", "user", user)

			c.set("program:pipelinePreemptedLogsHandler", "process_name", "%(program_name)s_%(process_num)s")
			c.set("program:pipelinePreemptedLogsHandler", "command",
			      "receivePipelineVmLogs --config {config} --subscription pipelineVmPreempted".format(config=config.path))
			c.set("program:pipelinePreemptedLogsHandler", "environment",
			      "PYTHONPATH={modulePath}".format(modulePath=MODULE_PATH))
			c.set("program:pipelinePreemptedLogsHandler", "numprocs", "10")  # TODO: come up with a formula for determining the number of processes
			c.set("program:pipelinePreemptedLogsHandler", "autostart", "true")
			c.set("program:pipelinePreemptedLogsHandler", "autorestart", "true")
			c.set("program:pipelinePreemptedLogsHandler", "user", user)

			c.set("program:pipelineDeleteLogsHandler", "process_name", "%(program_name)s_%(process_num)s")
			c.set("program:pipelineDeleteLogsHandler", "command",
			      "receivePipelineVmLogs --config {config} --subscription pipelineVmDelete".format(config=config.path))
			c.set("program:pipelineDeleteLogsHandler", "environment", "PYTHONPATH={modulePath}".format(modulePath=MODULE_PATH))
			c.set("program:pipelineDeleteLogsHandler", "numprocs", "10")  # TODO: come up with a formula for determining the number of processes
			c.set("program:pipelineDeleteLogsHandler", "autostart", "true")
			c.set("program:pipelineDeleteLogsHandler", "autorestart", "true")
			c.set("program:pipelineDeleteLogsHandler", "user", user)

			try:
				with open("/etc/supervisor/supervisord.conf", "w") as f:
					c.write(f)
			except IOError as e:
				print "ERROR: couldn't start the scheduler: {reason}".format(reason=e)
				exit(-1)

		try:
			subprocess.check_call(["sudo", "service", "supervisor", "restart"])

		except subprocess.CalledProcessError as e:
			print "ERROR: couldn't restart the scheduler (supervisor): {reason}".format(reason=e)
			exit(-1)

		print "Scheduler started successfully!"

	@staticmethod
	def stopScheduler():
		try:
			subprocess.check_call(["sudo", "service", "supervisor", "stop"])

		except subprocess.CalledProcessError as e:
			print "ERROR: couldn't stop the scheduler (supervisor): {reason}".format(reason=e)
			exit(-1)

		print "Scheduler stopped successfully!"

	@staticmethod
	def submitPipeline(args, config):

		# send the request message to the local rabbitmq server
		if args.scriptUrl:
			pipelineSpec = PipelineSchema(args.pipelineName, config, args.logsPath, args.imageName,
			                              scriptUrl=args.scriptUrl, cores=args.cores,
			                              mem=args.mem, diskSize=args.diskSize, diskType=args.diskType, env=args.env,
			                              inputs=args.inputs, outputs=args.outputs, tag=args.tag,
			                              preemptible=args.preemptible)
		elif args.cmd:
			pipelineSpec = PipelineSchema(args.pipelineName, config, args.logsPath, args.imageName, cmd=args.cmd,
			                              cores=args.cores,
			                              mem=args.mem, diskSize=args.diskSize, diskType=args.diskType, env=args.env,
			                              inputs=args.inputs, outputs=args.outputs, tag=args.tag,
			                              preemptible=args.preemptible)

		pipelineBuilder = PipelineBuilder(config)
		pipelineBuilder.addStep(pipelineSpec)
		pipelineBuilder.run()

	@staticmethod
	def stopPipeline(args, config):
		pipelineQueueUtils = PipelineQueueUtils('CANCEL_Q')

		pipelineDbUtils = PipelineDbUtils(config)

		if args.jobId:
			jobInfo = pipelineDbUtils.getJobInfo(select=["current_status", "operation_id", "job_id"],
			                                     where={"job_id": args.jobId})

		elif args.pipeline:
			jobInfo = pipelineDbUtils.getJobInfo(select=["current_status", "operation_id", "job_id"],
			                                     where={"pipeline_name": args.pipeline})

		elif args.tag:
			jobInfo = pipelineDbUtils.getJobInfo(select=["current_status", "operation_id", "job_id"],
			                                     where={"tag": args.tag})

		for j in jobInfo:
			if j.current_status == "RUNNING":
				msg = {
					"job_id": j.job_id,
					"operation_id": j.operation_id
				}
				pipelineQueueUtils.publish(json.dumps(msg))


	@staticmethod
	def restartJobs(args, config):  # TODO: reimplement
		pipelineDbUtils = PipelineDbUtils(config)
		pipelineQueueUtils = PipelineQueueUtils('WAIT_Q')

		if args.jobId:
			request = json.loads(pipelineDbUtils.getJobInfo(select=["request"], where={"job_id": args.jobId})[0].request)
			msg = {
				"job_id": args.jobId,
				"request": request
			}
			pipelineQueueUtils.publish(json.dumps(msg))

		if args.preempted:
			preempted = pipelineDbUtils.getJobInfo(select=["job_id", "request"], where={"current_status": "PREEMPTED"})

			for p in preempted:
				msg = {
					"job_id": p.job_id,
					"request": json.loads(p.request)
				}
				pipelineQueueUtils.publish(json.dumps(msg))

	@staticmethod
	def getJobLogs(args, config):  # TODO: reimplement
		pipelineDbUtils = PipelineDbUtils(config)

		jobInfo = pipelineDbUtils.getJobInfo(select=["stdout_log", "stderr_log", "gcs_log_path"],
		                                     where={"job_id": args.jobId})

		with open(os.devnull, 'w') as fnull:
			if args.stdout:
				try:
					stdoutLogFile = subprocess.check_output(
						["gsutil", "cat", os.path.join(jobInfo[0].gcs_log_path, jobInfo[0].stdout_log)], stderr=fnull)
				except subprocess.CalledProcessError as e:
					print "ERROR: couldn't get the stdout log : {reason}".format(reason=e)
					exit(-1)

				print "STDOUT:\n"
				print stdoutLogFile
				print "---------\n"

			if args.stderr:
				try:
					stderrLogFile = subprocess.check_output(
						["gsutil", "-q", "cat", os.path.join(jobInfo[0].gcs_log_path, jobInfo[0].stderr_log)],
						stderr=fnull)
				except subprocess.CalledProcessError as e:
					print "ERROR: couldn't get the stderr log : {reason}".format(reason=e)
					exit(-1)

				print "STDERR:\n"
				print stderrLogFile
				print "---------\n"

		pipelineDbUtils.closeConnection()

	@staticmethod
	def getJobsList(args, unknown, config):  # TODO: reimplement
		header = "JOBID%sPIPELINE%sOPERATION ID%sTAG%sSTATUS%sCREATE TIME%sPREEMPTIONS\n"
		jobStatusString = "{jobId}%s{pipeline}%s{operationId}%s{tag}%s{status}%s{createTime}%s{preemptions}\n"

		pipelineDbUtils = PipelineDbUtils(config)

		parser = argparse.ArgumentParser()
		parser.add_argument("--pipeline")
		parser.add_argument("--tag")
		parser.add_argument("--status", choices=["running", "waiting", "succeeded", "failed", "error", "preempted"])
		parser.add_argument("--createTimeAfter")
		parser.add_argument("--limit", default=50)

		args = parser.parse_args(args=unknown, namespace=args)

		select = ["job_id", "operation_id", "pipeline_name", "tag", "current_status", "create_time", "preemptions"]
		where = {}

		if args.pipeline:
			where["pipeline_name"] = args.pipeline

		if args.tag:
			where["tag"] = args.tag

		if args.status:
			where["current_status"] = args.status

		if args.createTimeAfter:
			where["create_time"] = {
				"value": str(args.createTimeAfter),
				"operator": ">="
			}

		jobs = pipelineDbUtils.getJobInfo(select=select, where=where)

		pipelineDbUtils.closeConnection()

		def fieldPadding(maxLen, actualLen):
			return ''.join([' ' for x in range(maxLen - actualLen + 4)])

		if len(jobs) > 0:
			jobIdLengths = [len(str(x.job_id)) if x.job_id is not None else len(str("None")) for x in jobs]
			jobIdLengths.append(len("JOBID"))
			maxJobIdLen = max(jobIdLengths)

			pipelineLengths = [len(x.pipeline_name) if x.pipeline_name is not None else len(str("None")) for x in jobs]
			pipelineLengths.append(len("PIPELINE"))
			maxPipelineLen = max(pipelineLengths)

			operationIdLengths = [len(x.operation_id) if x.operation_id is not None else len(str("None")) for x in jobs]
			operationIdLengths.append(len("OPERATION ID"))
			maxOperationIdLen = max(operationIdLengths)

			statusLengths = [len(x.current_status) if x.current_status is not None else len(str("None")) for x in jobs]
			statusLengths.append(len("STATUS"))
			maxStatusLen = max(statusLengths)

			tagLengths = [len(x.tag) if x.tag is not None else len(str("None")) for x in jobs]
			tagLengths.append(len("TAG"))
			maxTagLen = max(tagLengths)

			createTimeLengths = [len(x.create_time) if x.create_time is not None else len(str("None")) for x in jobs]
			createTimeLengths.append(len("CREATE TIME"))
			maxCreateTimeLen = max(createTimeLengths)

			print header % (fieldPadding(maxJobIdLen, len("JOBID")), fieldPadding(maxPipelineLen, len("PIPELINE")),
			                fieldPadding(maxOperationIdLen, len("OPERATION ID")), fieldPadding(maxTagLen, len("TAG")),
			                fieldPadding(maxStatusLen, len("STATUS")),
			                fieldPadding(maxCreateTimeLen, len("CREATE TIME")))
			for j in jobs:
				if j.create_time is not None:
					ct = j.create_time
				else:
					ct = "None"
				if j.operation_id is not None:
					op = j.operation_id
				else:
					op = "None"

				print jobStatusString.format(jobId=j.job_id, pipeline=j.pipeline_name, operationId=op, tag=j.tag,
				                             status=j.current_status, createTime=ct, preemptions=j.preemptions) % (
				      fieldPadding(maxJobIdLen, len(str(j.job_id))), fieldPadding(maxPipelineLen, len(j.pipeline_name)),
				      fieldPadding(maxOperationIdLen, len(op)), fieldPadding(maxTagLen, len(j.tag)),
				      fieldPadding(maxStatusLen, len(j.current_status)), fieldPadding(maxCreateTimeLen, len(ct)))

		else:
			print "No jobs found"

	@staticmethod
	def editPipeline(args, config):
		pipelineDbUtils = PipelineDbUtils(config)

		request = json.loads(pipelineDbUtils.getJobInfo(select=["request"], where={"job_id": args.jobId})[0].request)

		_, tmp = mkstemp()
		with open(tmp, 'w') as f:
			f.write("{data}".format(data=json.dumps(request, indent=4)))

		if "EDITOR" in os.environ.keys():
			editor = os.environ["EDITOR"]
		else:
			editor = "/usr/bin/nano"

		if subprocess.call([editor, tmp]) == 0:
			with open(tmp, 'r') as f:
				request = json.load(f)

			pipelineDbUtils.updateJob(args.jobId, keyName="job_id", setValues={"request": json.dumps(request)})
		else:
			print "ERROR: there was a problem editing the request"
			exit(-1)

	@staticmethod
	def writeStdout(s):
		ts = datetime.fromtimestamp(time()).strftime('%Y-%m-%d %H:%M:%S')
		sys.stdout.write('\t'.join([ts, s]) + '\n')
		sys.stdout.flush()

	@staticmethod
	def writeStderr(s):
		ts = datetime.fromtimestamp(time()).strftime('%Y-%m-%d %H:%M:%S')
		sys.stderr.write('\t'.join([ts, s]) + '\n')
		sys.stderr.flush()


class PipelineQueueUtils(object):
	def __init__(self, queueName):
		self._qname = queueName
		self._connection = pika.BlockingConnection(pika.ConnectionParameters(host='localhost'))
		self._channel = self._connection.channel()
		self._channel.queue_declare(queue=queueName, durable=True)

	def __del__(self):
		self._connection.close()

	def publish(self, msg):
		self._channel.basic_publish(exchange='', routing_key=self._qname, body=msg)

	def get(self):
		self._channel.basic_qos(prefetch_count=1)
		method, header, body = self._channel.basic_get(self._qname)

		return body, method

	def acknowledge(self, method):
		if method:
			self._channel.basic_ack(method.delivery_tag)
		else:
			PipelineSchedulerUtils.writeStdout("No message returned!")


class PipelineDbUtils(object):
	def __init__(self, config):
		if config.db == "mysql":
			pass  # TODO: determine best production grade relational database to use
			#self._dbConn = MySQLdb.connect(host=config.db_host, user=config.db_user, passwd=config.db_password, db="pipelines", port=config.db_port)
		elif config.db == "sqlite":
			self._dbConn = sqlite3.connect(os.path.join(os.path.dirname(config.path), "isb-cgc-pipelines.db"))

		self._pipelinesDb = self._dbConn.cursor()

	def __del__(self):
		self._dbConn.close()

	def closeConnection(self):
		self._dbConn.close()

	def insertJob(self, *args):
		self._pipelinesDb.execute("INSERT INTO jobs (operation_id, instance_name, pipeline_name, tag, current_status, preemptions, gcs_log_path, stdout_log, stderr_log, create_time, end_time, processing_time, request) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)", tuple(args))
		self._dbConn.commit()

		return self._pipelinesDb.lastrowid

	def insertJobDependency(self, parentId, childId):
		self._pipelinesDb.execute("INSERT INTO job_dependencies (parent_id, child_id) VALUES (?,?)", (parentId, childId))
		self._dbConn.commit()

	def updateJob(self, key, setValues, keyName="operation_id"):  # setValues -> dict
		if "preemptions" in setValues.keys():
			query = "UPDATE jobs SET preemptions = preemptions + 1 WHERE {key} = ?".format(key=keyName)
			self._pipelinesDb.execute(query, (key,))
			setValues.pop("preemptions")

		query = "UPDATE jobs SET {values} WHERE {key} = ?".format(key=keyName, values=','.join(["{v} = ?".format(v=v) for v in setValues.iterkeys()]))

		self._pipelinesDb.execute(query, tuple(setValues.itervalues()) + (key,))
		self._dbConn.commit()

	def getParentJobs(self, childId):
		return self._pipelinesDb.execute("SELECT parent_id FROM job_dependencies WHERE child_id = ?", (childId,)).fetchall()

	def getChildJobs(self, parentId):
		return self._pipelinesDb.execute("SELECT child_id FROM job_dependencies WHERE parent_id = ?", (parentId,)).fetchall()

	def getJobInfo(self, select=None, where=None, operation="intersection"):  # select -> list, where -> dict
		class JobInfo(object):
			def __init__(self, innerDict):
				self.__dict__.update(innerDict)

		operations = {
			"union": "OR",
			"intersection": "AND"
		}

		query = "SELECT {select} FROM jobs"
		params = []

		if select is None: 
			selectString = "*"

		else:
			selectString = ','.join(select)

		if where is None:
			whereString = ""
		
		else:
			query += " WHERE {where}"
			whereArray = []
			valueArray = []

			for k in where.iterkeys():
				if type(where[k]) == "dict":
					try:
						whereArray.append("{k} {comp} ?".format(k=k, comp=where[k]["comparison"]))
						valueArray.append(where[k]["value"])
					except KeyError as e:
						PipelineSchedulerUtils.writeStderr("ERROR: problem getting job info: {reason}".format(reason=e))

				else:
					whereArray.append("{k} = ?".format(k=k))
					valueArray.append(where[k])

			whereString = ' {op} '.format(op=operations[operation]).join(whereArray)

			params.extend(valueArray)

		jobsInfo = self._pipelinesDb.execute(query.format(select=selectString, where=whereString), tuple(params)).fetchall()

		jobsList = []
		for j in jobsInfo:
			newDict = {}
			if select is None:
				select = ["job_id", "operation_id", "pipeline_name", "tag", "current_status", "preemptions", "gcs_log_path", "stdout_log", "stderr_log", "create_time", "end_time", "processing_time", "request"]

			for i, k in enumerate(select):
				newDict[k] = j[i]

			jobsList.append(JobInfo(newDict))

		return jobsList

	def createJobTables(self):
		if len(self._pipelinesDb.execute('SELECT name FROM sqlite_master WHERE type="table" AND name="jobs"').fetchall()) == 0:
			query = (
				'CREATE TABLE jobs (job_id INTEGER PRIMARY KEY AUTOINCREMENT, '
				'operation_id VARCHAR(128), '
				'instance_name VARCHAR(128), '
				'pipeline_name VARCHAR(128), '
				'tag VARCHAR(128), '
				'current_status VARCHAR(128), '
				'preemptions INTEGER, '
				'gcs_log_path VARCHAR(128), '
				'stdout_log VARCHAR(128), '
				'stderr_log VARCHAR(128), '
				'create_time VARCHAR(128), '
				'end_time VARCHAR(128), '
				'processing_time FLOAT, '
				'request TEXT)'
			)
			self._pipelinesDb.execute(query)
			self._dbConn.commit()

		if len(self._pipelinesDb.execute('SELECT name FROM sqlite_master WHERE type="table" AND name="job_dependencies"').fetchall()) == 0:
			self._pipelinesDb.execute("CREATE TABLE job_dependencies (row_id INTEGER PRIMARY KEY AUTOINCREMENT, parent_id INTEGER, child_id INTEGER)")
			self._dbConn.commit()


class PipelineServiceUtils:

	@staticmethod
	def bootstrapMessageHandlers(pubsub, logging, config, mode="local"):  # TODO: move topics that aren't required to the configuration file
		# create log sinks for pipeline vm logs
		timestamp = datetime.utcnow().isoformat("T") + "Z"  # RFC3339 timestamp

		topics = {
			"pipelineVmInsert": {
				"filter": ('resource.type="gce_instance" AND '
						'timestamp > "{tz}" AND jsonPayload.resource.name:"ggp-" AND '
						'jsonPayload.event_type="GCE_OPERATION_DONE" AND '
						'jsonPayload.event_subtype="compute.instances.insert" AND '
						'NOT error AND logName="projects/{project}/logs/compute.googleapis.com%2Factivity_log"'
				).format(project=config.project_id, tz=timestamp),
				"trigger": "topic"
			},
			"pipelineVmPreempted": {
				"filter": ('resource.type="gce_instance" AND '
						'timestamp > "{tz}" AND jsonPayload.resource.name:"ggp-" AND '
						'jsonPayload.event_type="GCE_OPERATION_DONE" AND '
						'jsonPayload.event_subtype="compute.instances.preempted" AND '
						'NOT error AND logName="projects/{project}/logs/compute.googleapis.com%2Factivity_log"'
				).format(project=config.project_id, tz=timestamp),
				"trigger": "topic"
			},
			"pipelineVmDelete": {
				"filter": ('resource.type="gce_instance" AND '
						'timestamp > "{tz}" AND jsonPayload.resource.name:"ggp-" AND '
						'jsonPayload.event_type="GCE_OPERATION_DONE" AND '
						'jsonPayload.event_subtype="compute.instances.delete" AND '
						'NOT error AND logName="projects/{project}/logs/compute.googleapis.com%2Factivity_log"'
				).format(project=config.project_id, tz=timestamp),
				"trigger": "topic",
			}
		}

		for t, v in topics.iteritems():
			topic = "projects/{project}/topics/{t}".format(project=config.project_id, t=t)
			subscription = 'projects/{project}/subscriptions/{subscription}'.format(project=config.project_id, subscription=t)
			try:
				pubsub.projects().topics().get(topic=topic).execute()
			except HttpError:
				try:
					pubsub.projects().topics().create(name=topic, body={"name": topic}).execute()
				except HttpError as e:
					print "ERROR: couldn't create pubsub topic {t} : {reason}".format(t=t, reason=e)
					exit(-1)

			try:
				pubsub.projects().subscriptions().get(subscription=subscription).execute()
			except HttpError:
				body = {
					"topic": topic,
					"name": subscription
				}
				try:
					pubsub.projects().subscriptions().create(name=subscription, body=body).execute()
				except HttpError as e:
					print "ERROR: couldn't create pubsub subscription {s}: {reason}".format(s=subscription, reaosn=e)
					exit(-1)

			body = {
				"destination": "pubsub.googleapis.com/projects/{project}/topics/{t}".format(project=config.project_id, t=t),
				"filter": v["filter"],
				"name": t,
				"outputVersionFormat": "V2"
			}

			sink = "projects/{project}/sinks/{t}".format(project=config.project_id, t=t)
			try:
				logging.projects().sinks().get(sinkName=sink).execute()
			except HttpError as e:
				try:
					logging.projects().sinks().create(projectName="projects/{project}".format(project=config.project_id), body=body).execute()
				except HttpError as e:
					print "ERROR: couldn't create the {t} log sink : {reason}".format(t=t, reason=e)
					exit(-1)

		print "Messaging bootstrap successful!"


class DataUtils(object):
	@staticmethod
	def getAnalysisDetail(analysisId):
		cghubMetadataUrl = "https://cghub.ucsc.edu/cghub/metadata/analysisDetail?analysis_id={analysisId}"

		headers = {
			"Accept": "application/json"
		}

		return requests.get(cghubMetadataUrl.format(analysisId=analysisId), headers=headers).json()

	@staticmethod
	def getFilenames(analysisId):
		analysisDetail = DataUtils.getAnalysisDetail(analysisId)
		files = []
		filenames = []
		if len(analysisDetail["result_set"]["results"]) > 0:
			files = analysisDetail["result_set"]["results"][0]["files"]

		for f in files:
			filenames.append(f["filename"])

		return filenames

	@staticmethod
	def calculateDiskSize(inputFile=None, inputFileSize=None, analysisId=None, scalingFactor=None, roundToNearestGbInterval=None):
		if inputFile is not None:
			fileSize = int(subprocess.check_output(["gsutil", "du", inputFile]).split(' ')[0])
		
		elif analysisId is not None:
			analysisDetail = DataUtils.getAnalysisDetail(analysisId)

			if len(analysisDetail["result_set"]["results"]) > 0:
				files = analysisDetail["result_set"]["results"][0]["files"]
				fileSize = sum([int(x["filesize"]) for x in files])
			else:
				print "ERROR: no files found for analysis ID {a}!".format(a=analysisId)
				exit(-1)

		if scalingFactor is not None:
			scalingFactor = int(scalingFactor)
		else:
			scalingFactor = 1

		if roundToNearestGbInterval is not None:
			roundTo = float(roundToNearestGbInterval) * 1000000000

		return int(math.ceil(scalingFactor * fileSize/roundTo)*roundTo)/1000000000

	@staticmethod
	def constructObjectPath(analysisId, outputPath):

		analysisDetail = DataUtils.getAnalysisDetail(analysisId)

		if len(analysisDetail["result_set"]["results"]) > 0:
			diseaseCode = analysisDetail["result_set"]["results"][0]["disease_abbr"]
			analyteCode = analysisDetail["result_set"]["results"][0]["analyte_code"]
			libraryStrategy = analysisDetail["result_set"]["results"][0]["library_strategy"]
			centerName = analysisDetail["result_set"]["results"][0]["center_name"]
			platform = analysisDetail["result_set"]["results"][0]["platform"]

			if analyteCode == "D":
				analyteType = "DNA"
			elif analyteCode == "X":
				analyteType = "WGA_RepliGX"
			elif analyteCode == "W":
				analyteType = "WGA_RepliG"
			elif analyteCode == "R":
				analyteType = "RNA"
			elif analyteCode == "T":
				analyteType = "Total_RNA"
			elif analyteCode == "H":
				analyteType = "hybrid_RNA"
			elif analyteCode == "G":
				analyteType = "WGA_GenomePlex"

			objectPath = "{diseaseCode}/{analyteType}/{libraryStrategy}/{centerName}/{platform}/".format(diseaseCode=diseaseCode, analyteType=analyteType, libraryStrategy=libraryStrategy, centerName=centerName, platform=platform)

			return os.path.join(outputPath, objectPath)

		else:
			raise LookupError("ERROR: no files found for analysis ID {a}!".format(a=analysisId))

	@staticmethod
	def getChecksum(analysisId):
		analysisDetail = DataUtils.getAnalysisDetail(analysisId)
		if len(analysisDetail["result_set"]["results"]) > 0:
			for f in analysisDetail["result_set"]["results"][0]["files"]:
				return f["checksum"]["#text"]
		else:
			raise LookupError("ERROR: no files found for analysis ID {a}!".format(a=analysisId))

