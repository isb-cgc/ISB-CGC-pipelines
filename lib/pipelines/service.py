import os
import re
import json
import string
import requests
import subprocess
import futures
from time import time, sleep
from random import SystemRandom
from datetime import datetime
from jinja2 import Template
from googleapiclient.errors import HttpError
from pipelines.paths import *
from pipelines.schema import PipelineSchema
from pipelines.builder import PipelineBuilder, PipelineSchemaValidationError, PipelineSubmitError
from pipelines.db import PipelineDatabase, PipelineDatabaseError
from pipelines.queue import PipelineQueue, PipelineExchange
from ConfigParser import SafeConfigParser


# Kubernetes API URIs
API_ROOT = "http://localhost:8080"
NAMESPACE_URI = "/api/v1/namespaces/"
PODS_URI = "/api/v1/namespaces/{namespace}/pods/"
SERVICES_URI = "/api/v1/namespaces/{namespace}/services/"
REPLICATION_CONTROLLERS_URI = "/api/v1/namespaces/{namespace}/replicationcontrollers/"
PERSISTENT_VOLUMES_URI = "/api/v1/persistentvolumes/"
PERSISTENT_VOLUME_CLAIMS_URI = "/api/v1/namespaces/{namespace}/persistentvolumeclaims/"
SECRETS_URI = "/api/v1/namespaces/{namespace}/secrets/"

SESSION = requests.Session()
API_HEADERS = {
	"Content-type": "application/json"
}


class PipelineServiceError(Exception):
	def __init__(self, msg):
		super(PipelineServiceError, self).__init__()
		self.msg = msg


class PipelineService:
	@staticmethod
	def startSupervisorScheduler(config, user):
		pipelineDatabase = PipelineDatabase(config)

		try:
			pipelineDatabase.createJobTables()
		except PipelineDatabaseError as e:
			print e
			exit(-1)

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
			c.set("program:pipelineJobScheduler", "command", "pipelineJobScheduler")
			c.set("program:pipelineJobScheduler", "environment", "PYTHONPATH={modulePath}".format(modulePath=MODULE_PATH))
			c.set("program:pipelineJobScheduler", "numprocs", "1")
			c.set("program:pipelineJobScheduler", "autostart", "true")
			c.set("program:pipelineJobScheduler", "autorestart", "true")
			c.set("program:pipelineJobScheduler", "user", user)

			c.set("program:pipelineJobCanceller", "process_name", "pipelineJobCanceller")
			c.set("program:pipelineJobCanceller", "command", "pipelineJobCanceller")
			c.set("program:pipelineJobCanceller", "environment", "PYTHONPATH={modulePath}".format(modulePath=MODULE_PATH))
			c.set("program:pipelineJobCanceller", "numprocs", "1")
			c.set("program:pipelineJobCanceller", "autostart", "true")
			c.set("program:pipelineJobCanceller", "autorestart", "true")
			c.set("program:pipelineJobCanceller", "user", user)

			c.set("program:pipelineInsertLogsHandler", "process_name", "%(program_name)s_%(process_num)s")
			c.set("program:pipelineInsertLogsHandler", "command", "receivePipelineVmLogs --subscription pipelineVmInsert")
			c.set("program:pipelineInsertLogsHandler", "environment", "PYTHONPATH={modulePath}".format(modulePath=MODULE_PATH))
			c.set("program:pipelineInsertLogsHandler", "numprocs", "10")  # TODO: add to client config
			c.set("program:pipelineInsertLogsHandler", "autostart", "true")
			c.set("program:pipelineInsertLogsHandler", "autorestart", "true")
			c.set("program:pipelineInsertLogsHandler", "user", user)

			c.set("program:pipelinePreemptedLogsHandler", "process_name", "%(program_name)s_%(process_num)s")
			c.set("program:pipelinePreemptedLogsHandler", "command", "receivePipelineVmLogs --subscription pipelineVmPreempted")
			c.set("program:pipelinePreemptedLogsHandler", "environment", "PYTHONPATH={modulePath}".format(modulePath=MODULE_PATH))
			c.set("program:pipelinePreemptedLogsHandler", "numprocs", "10")  # TODO: add to client config
			c.set("program:pipelinePreemptedLogsHandler", "autostart", "true")
			c.set("program:pipelinePreemptedLogsHandler", "autorestart", "true")
			c.set("program:pipelinePreemptedLogsHandler", "user", user)

			c.set("program:pipelineDeleteLogsHandler", "process_name", "%(program_name)s_%(process_num)s")
			c.set("program:pipelineDeleteLogsHandler", "command", "receivePipelineVmLogs --subscription pipelineVmDelete")
			c.set("program:pipelineDeleteLogsHandler", "environment", "PYTHONPATH={modulePath}".format(modulePath=MODULE_PATH))
			c.set("program:pipelineDeleteLogsHandler", "numprocs", "10")  # TODO: add to client config
			c.set("program:pipelineDeleteLogsHandler", "autostart", "true")
			c.set("program:pipelineDeleteLogsHandler", "autorestart", "true")
			c.set("program:pipelineDeleteLogsHandler", "user", user)

			with open("/etc/supervisor/supervisord.conf", "w") as f:
				c.write(f)

		try:
			subprocess.check_call(["sudo", "service", "supervisor", "restart"])

		except subprocess.CalledProcessError as e:
			print "ERROR: couldn't restart the scheduler (supervisor): {reason}".format(reason=e)
			exit(-1)

		print "Scheduler started successfully!"

	@staticmethod
	def stopSupervisorScheduler():
		try:
			subprocess.check_call(["sudo", "service", "supervisor", "stop"])

		except subprocess.CalledProcessError as e:
			print "ERROR: couldn't stop the scheduler (supervisor): {reason}".format(reason=e)
			exit(-1)

		print "Scheduler stopped successfully!"

	@staticmethod
	def watchJob(jobId, exchangeName):
		queue = PipelineQueue('PIPELINE_JOB_{j}'.format(j=jobId))
		queue.bindToExchange(exchangeName, jobId)

		diskName = None

		while True:
			body, method = queue.get()

			if method:
				body = json.loads(body)

				if body["current_status"] == "SUCCEEDED":
					pass  # TODO: get the name of the disk
				else:
					raise PipelineServiceError("Job {j} has current status {s}!".format(j=jobId, s=body["current_status"]))
			else:
				pass

		return diskName


	@staticmethod
	def bootstrapServiceCluster(gke, credentials, http, projectId, zone, clusterName, machineType, nodes, nodeDiskSize, nodeDiskType):

		def refreshAccessToken():
			if credentials.access_token_expired:
				credentials.refresh(http)

		def prepareTemplate(fp, **kwargs):
			s = fp.read().replace('\n', '')
			t = Template(s)
			return json.loads(t.render(**kwargs))

		def createResource(url, headers, resource):
			response = SESSION.post(url, headers=headers, json=resource)

			# if the response isn't what's expected, raise an exception
			if response.status_code != 201:
				if response.status_code == 409:  # already exists
					pass
				else:
					print "ERROR: Namespace creation failed: {reason}".format(reason=response.status_code)
					exit(-1)  # probably should raise an exception

		def createClusterAdminPassword():
			return ''.join(SystemRandom().choice(string.ascii_uppercase + string.ascii_lowercase + string.digits) for _ in range(16))

		def clusterExists():
			clusterExists = False

			try:
				response = gke.projects().zones().clusters().get(projectId=projectId, zone=zone, clusterId=clusterName).execute()

			except HttpError:
				pass

			else:
				# make sure the existing cluster meets the requirements in the given specification
				try:
					if str(response["name"]) == str(clusterName) and \
							str(response["initialNodeCount"]) == str(nodes) and \
							str(response["nodeConfig"]["diskSizeGb"]) == str(nodeDiskSize) and \
							str(response["nodeConfig"]["machineType"]) == str(machineType):

						clusterExists = True

				except KeyError:
					pass

			return clusterExists

		def ensureCluster():
			# create a cluster to run the workflow on if it doesn't exist already
			if not clusterExists():
				print "Creating cluster {cluster} ...".format(cluster=clusterName)
				createCluster = gke.projects().zones().clusters().create(projectId=projectId, zone=zone, body=clusterSpec).execute()

				# wait for the operation to complete
				while True:
					response = gke.projects().zones().operations().get(projectId=projectId, zone=zone, operationId=createCluster["name"]).execute()

					if response['status'] == 'DONE':
						break
					else:
						sleep(1)

		def configureClusterAccess():
			# configure cluster access (may want to perform checks before doing this)
			print "Configuring access to cluster {cluster} ...".format(cluster=clusterName)

			# configure ssh keys
			try:
				subprocess.check_call(["bash", "-c", "stat ~/.ssh/google_compute_engine && stat ~/.ssh/google_compute_engine.pub"])
			except subprocess.CalledProcessError as e:
				try:
					subprocess.check_call(["gcloud", "compute", "config-ssh"])
				except subprocess.CalledProcessError as e:
					print "ERROR: Couldn't generate SSH keys for the workstation: {e}".format(e=e)
					exit(-1)

			try:
				subprocess.check_call(["gcloud", "config", "set", "compute/zone", zone])
			except subprocess.CalledProcessError as e:
				print "ERROR: Couldn't set the compute zone: {reason}".format(reason=e)
				exit(-1)

			try:
				subprocess.check_call(["kubectl", "config", "set", "cluster", clusterName])
			except subprocess.CalledProcessError as e:
				print "ERROR: Couldn't set cluster in configuration: {reason}".format(reason=e)
				exit(-1)  # raise an exception

			try:
				subprocess.check_call(["gcloud", "container", "clusters", "get-credentials", clusterName])
			except subprocess.CalledProcessError as e:
				print "ERROR: Couldn't get cluster credentials: {reason}".format(reason=e)
				exit(-1)  # raise an exception

			# get the cluster hosts for reference
			try:
				instanceGroupName = subprocess.check_output(["gcloud", "compute", "instance-groups", "list", "--regexp", "^gke-{cluster}-.*-group$".format(cluster=clusterName)]).split('\n')[1].split(' ')[0]
				instanceList = subprocess.check_output(["gcloud", "compute", "instance-groups", "list-instances", instanceGroupName]).split('\n')[1:-1]
				for instance in instanceList:
					pass  # TODO: set cluster hosts in client configuration

			except subprocess.CalledProcessError as e:
				print "ERROR: Couldn't get cluster hostnames: {reason}".format(reason=e)
				exit(-1)  # raise an exception

			# run kubectl in proxy mode in a background process
			try:
				subprocess.Popen(["kubectl", "proxy", "--port", "8080"])  # TODO: document the in-use ports, and/or figure out how to "detect" them (manage in configuration?)
			except ValueError as e:
				print "ERROR: couldn't set up kubectl proxy: {reason}".format(reason=e)
				exit(-1)

			# make sure the proxy is running -- something is going wrong here that I can't figure out.
			timeout = 180
			while True:
				try:
					response = SESSION.get(API_ROOT + NAMESPACE_URI)
				except:
					continue

				if response.status_code == 200:
					break

				if timeout <= 0:
					print "ERROR: Couldn't access proxy (timeout reached): {status}".format(status=response.content)
					exit(-1)
				else:
					sleep(1)
					timeout -= 1

			# create a namespace for the workflow
			url = API_ROOT + NAMESPACE_URI
			createResource(url, API_HEADERS, namespaceSpec)

			# set the namespace for the current context if it doesn't exist already
			kubectlContext = subprocess.Popen(["kubectl", "config", "current-context"], stdout=subprocess.PIPE)

			try:
				subprocess.check_call(["kubectl", "config", "set", "contexts.{context}.namespace".format(context=kubectlContext), namespaceSpec["metadata"]["name"]])
			except subprocess.CalledProcessError as e:
				print "ERROR: Couldn't set cluster context: {reason}".format(reason=e)
				exit(-1)  # raise an exception

			print "Cluster configuration was successful!"

		# authentication
		refreshAccessToken()
		API_HEADERS.update({"Authorization": "Bearer {token}".format(token=credentials.access_token)})

		# load/render/submit the request templates

		# cluster template
		with open(os.path.join(TEMPLATES_PATH, "cluster.json.jinja2")) as f:
			clusterSpec = prepareTemplate(f, projectId=projectId, zone=zone, clusterName=clusterName, nodes=nodes, nodeDiskSize=nodeDiskSize, nodeDiskType=nodeDiskType)

		# namespace template
		with open(os.path.join(TEMPLATES_PATH, "namespaces.json.jinja2")) as f:
			namespaceSpec = prepareTemplate(f, name=clusterName)

		# create and configure cluster
		ensureCluster()
		configureClusterAccess()

		serviceUrl = API_ROOT + SERVICES_URI.format(namespace=namespaceSpec["metadata"]["name"])
		rcUrl = API_ROOT + REPLICATION_CONTROLLERS_URI.format(namespace=namespaceSpec["metadata"]["name"])
		volumeUrl = API_ROOT + PERSISTENT_VOLUMES_URI.format(namespace=namespaceSpec["metadata"]["name"])

		# pipeline front-end service
		path = os.path.join(TEMPLATES_PATH, "pipeline-frontend-service.json.jinja2")
		with open(path) as f:
			pipelineFrontendServiceSpec = prepareTemplate(f)  # TODO: kwargs

		createResource(serviceUrl, API_HEADERS, pipelineFrontendServiceSpec)

		# sqlite-reader-service
		with open(os.path.join(TEMPLATES_PATH, "sqlite-reader-service.json.jinja2")) as f:
			sqliteReaderServiceSpec = prepareTemplate(f)  # TODO: kwargs

		createResource(serviceUrl, API_HEADERS, sqliteReaderServiceSpec)

		# sqlite-writer-service
		with open(os.path.join(TEMPLATES_PATH, "sqlite-writer-service.json.jinja2")) as f:
			sqliteWriterServiceSpec = prepareTemplate(f)  # TODO: kwargs

		createResource(serviceUrl, API_HEADERS, sqliteWriterServiceSpec)

		# rabbitmq service
		with open(os.path.join(TEMPLATES_PATH, "rabbitmq-service.json.jinja2")) as f:
			rabbitmqServiceSpec = prepareTemplate(f)  # TODO: kwargs

		createResource(serviceUrl, API_HEADERS, rabbitmqServiceSpec)

		# pipeline front-end rc
		with open(os.path.join(TEMPLATES_PATH, "pipeline-frontend-rc.json.jinja2")) as f:
			pipelineFrontendRcSpec = prepareTemplate(f)  # TODO: kwargs

		createResource(rcUrl, API_HEADERS, pipelineFrontendRcSpec)

		# rabbitmq rc
		with open(os.path.join(TEMPLATES_PATH, "rabbitmq-rc.json.jinja2")) as f:
			rabbitmqRcSpec = prepareTemplate(f)  # TODO: kwargs

		createResource(rcUrl, API_HEADERS, rabbitmqRcSpec)

		# temporary (hostpath) sqlite volume
		with open(os.path.join(TEMPLATES_PATH, "temporary-sqlite-volume.json.jinja2")) as f:
			temporarySqliteVolume = prepareTemplate(f)  # TODO: kwargs

		# temporary (hostpath) config volume
		with open(os.path.join(TEMPLATES_PATH, "temporary-sqlite-volume.json.jinja2")) as f:
			temporarySqliteVolume = prepareTemplate(f)  # TODO: kwargs

		# temporary sqlite-reader-rc
		with open(os.path.join(TEMPLATES_PATH, "temporary-sqlite-reader-rc.json.jinja2")) as f:
			temporarySqliteReaderRcSpec = prepareTemplate(f)  # TODO: kwargs

		createResource(rcUrl, API_HEADERS, temporarySqliteReaderRcSpec)

		# temporary sqlite-writer-rc
		with open(os.path.join(TEMPLATES_PATH, "temporary-sqlite-writer-rc.json.jinja2")) as f:
			temporarySqliteWriterRcSpec = prepareTemplate(f)  # TODO: kwargs

		createResource(rcUrl, API_HEADERS, temporarySqliteWriterRcSpec)

		# temporary config-writer-rc
		with open(os.path.join(TEMPLATES_PATH, "temporary-config-writer-rc.json.jinja2")) as f:
			temporaryConfigWriterRcSpec = prepareTemplate(f)  # TODO: kwargs

		createResource(rcUrl, API_HEADERS, temporaryConfigWriterRcSpec)

		# pipeline log handler rc
		with open(os.path.join(TEMPLATES_PATH, "pipeline-log-handler-rc.json.jinja2")) as f:
			pipelineLogHandlerRcSpec = prepareTemplate(f)  # TODO: kwargs

		createResource(rcUrl, API_HEADERS, pipelineLogHandlerRcSpec)

		# pipeline scheduler rc
		with open(os.path.join(TEMPLATES_PATH, "pipeline-scheduler-rc.json.jinja2")) as f:
			pipelineSchedulerRcSpec = prepareTemplate(f)  # TODO: kwargs

		createResource(rcUrl, API_HEADERS, pipelineSchedulerRcSpec)

		# pipeline canceller rc
		with open(os.path.join(TEMPLATES_PATH, "pipeline-canceller-rc.json.jinja2")) as f:
			pipelineCancellerRcSpec = prepareTemplate(f)  # TODO: kwargs

		createResource(rcUrl, API_HEADERS, pipelineCancellerRcSpec)

		# TODO: port forward the rabbitmq service to a local port

		# set up the watch
		PipelineExchange('WATCH_EXCHANGE')

		# run pipeline jobs for creating/populating the service disks
		createDisks = PipelineBuilder()

		configDiskJob = PipelineSchema()
		databaseDiskJob = PipelineSchema()

		createDisks.addStep(configDiskJob)
		createDisks.addStep(databaseDiskJob)

		try:
			jobIds = createDisks.run()

		except PipelineSchemaValidationError as e:
			pass  # TODO: print error and exit

		except PipelineSubmitError as e:
			pass  # TODO: print error and exit

		# set a watch on each job id and wait for the jobs to complete
		error = 0
		errors = []
		diskNames = []

		with futures.ThreadPoolExecutor(50) as p:
			statuses = dict((p.submit(PipelineService.watchJob, j, 'WATCH_EXCHANGE') for j in jobIds))

			for s in futures.as_completed(statuses):
				if s.exception() is not None:
					errors.append("ERROR: Couldn't create disk volume: {reason}".format(reason=s.exception()))
					error = 1
				else:
					diskNames.append(s.result())

		if error == 1:
			for e in errors:
				print e

			exit(-1)

		for d in diskNames:
			if re.match('^sqlite.*', d):
				pass  # TODO: create permanent sqlite volume
			elif re.match('^config.*', d):
				pass  # TODO: create permanent config volume

		# restart the sqlite RCs with the newly created volumes

		print "Service bootstrap successful!"


	@staticmethod
	def bootstrapMessageHandlers(pubsub, logging, config):
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