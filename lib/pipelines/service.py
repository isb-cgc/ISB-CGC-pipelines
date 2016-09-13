import json
import uuid
import string
import requests
import httplib2
import subprocess
from concurrent.futures import ThreadPoolExecutor, as_completed
from time import sleep
from random import SystemRandom
from datetime import datetime
from jinja2 import Template
from googleapiclient import discovery
from oauth2client.client import GoogleCredentials
from googleapiclient.errors import HttpError
from paths import *
from schema import PipelineSchema
from builder import PipelineBuilder, PipelineSchemaValidationError, PipelineSubmitError
from queue import PipelineQueue, PipelineExchange
from config import PipelineConfig, PipelineConfigError


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


class DataDiskError(Exception):
	def __init__(self, msg):
		super(DataDiskError, self).__init__()
		self.msg = msg


class DataDisk(object):
	@staticmethod
	def create(config, name=None, size=None, type="PERSISTENT_SSD", zone=None):
		# submit a request to the gce api for a new disk with the given parameters
		# if inputs is not None, run a pipeline job to populate the disk
		projectId = config.project_id

		credentials = GoogleCredentials.get_application_default()
		http = credentials.authorize(httplib2.Http())

		if credentials.access_token_expired:
			credentials.refresh(http)

		gce = discovery.build('compute', 'v1', http=http)

		diskTypes = {
			"PERSISTENT_HDD": "pd-standard",
			"PERSISTENT_SSD": "pd-ssd"
		}

		body = {
			"kind": "compute#disk",
			"zone": "projects/{projectId}/zones/{zone}".format(projectId=projectId, zone=zone),
			"name": name,
			"sizeGb": size,
			"type": "projects/{projectId}/zones/{zone}/diskTypes/{type}".format(projectId=projectId, zone=zone,
			                                                                    type=diskTypes[type])
		}

		try:
			resp = gce.disks().insert(project=projectId, zone=zone, body=body).execute()
		except HttpError as e:
			raise DataDiskError("Couldn't create data disk {n}: {reason}".format(n=name, reason=e))

		while True:
			try:
				result = gce.zoneOperations().get(project=projectId, zone=zone, operation=resp['name']).execute()
			except HttpError:
				break
			else:
				if result['status'] == 'DONE':
					break

		return "projects/{p}/zones/{z}/disks/{d}".format(p=config.project_id, zones=zone, d=name)

	@staticmethod
	def delete(config, disk_name=None, disk_zone=None):  # TODO: implement
		# submit a request to the gce api for a new disk with the given parameters
		# if inputs is not None, run a pipeline job to populate the disk
		projectId = config.project_id
		zones = [disk_zone if disk_zone is not None else x for x in config.zones.split(',')]

		credentials = GoogleCredentials.get_application_default()
		http = credentials.authorize(httplib2.Http())

		if credentials.access_token_expired:
			credentials.refresh(http)

		gce = discovery.build('compute', 'v1', http=http)

		for z in zones:
			try:
				resp = gce.disks().delete(project=projectId, zone=z, disk=disk_name).execute()
			except HttpError as e:
				raise DataDiskError("Couldn't delete data disk {n}: {reason}".format(n=disk_name, reason=e))

			while True:
				try:
					result = gce.zoneOperations().get(project=projectId, zone=z, operation=resp['name']).execute()
				except HttpError:
					break
				else:
					if result['status'] == 'DONE':
						break


class PipelineServiceError(Exception):
	def __init__(self, msg):
		super(PipelineServiceError, self).__init__()
		self.msg = msg


class PipelineService:
	@staticmethod
	def sendRequest(ip, port, route, data=None, protocol="http"):
		url = "{protocol}://{ip}:{port}{route}".format(protocol=protocol, ip=ip, port=port, route=route)

		if data is not None:
			try:
				resp = requests.post(url, data=data)

			except requests.HTTPError as e:
				raise PipelineServiceError("{reason}".format(reason=e))

		else:
			try:
				resp = requests.get(url)

			except requests.HTTPError as e:
				raise PipelineServiceError("{reason}".format(reason=e))

		return resp

	@staticmethod
	def watchJob(jobId, exchangeName):
		queue = PipelineQueue('PIPELINE_JOB_{j}'.format(j=jobId))
		queue.bindToExchange(exchangeName, jobId)

		while True:
			body, method = queue.get()

			if method:
				body = json.loads(body)

				if body["current_status"] == "SUCCEEDED":
					return jobId
				else:
					raise PipelineServiceError("Job {j} has current status {s}!".format(j=jobId, s=body["current_status"]))
			else:
				pass


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
			if response.status_code != 200:
				print "ERROR: resource creation failed: {reason}".format(reason=response.status_code)
				exit(-1)  # probably should raise an exception

		def deleteResource(url, headers, resourceName):
			response = SESSION.delete(os.path.join(url, resourceName), headers=headers)
			# if the response isn't what's expected, raise an exception
			if response.status_code != 200:
				print "ERROR: resource deletion failed: {reason}".format(reason=response.status_code)
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

			except subprocess.CalledProcessError as e:
				print "ERROR: Couldn't get cluster hostnames: {reason}".format(reason=e)
				exit(-1)  # raise an exception

			# run kubectl in proxy mode in a background process
			try:
				subprocess.Popen(["kubectl", "proxy", "--port", "8080"])  # TODO: document the in-use ports, and/or figure out how to "detect" them (manage in configuration?); Also may need to run this command every time you log in...
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

		# dummy config
		config = PipelineConfig(project_id=projectId)

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

		try:
			node = subprocess.check_output(["kubectl", "get", "nodes"]).split('\n')[1].split(' ')[0]
		except subprocess.CalledProcessError as e:
			print "ERROR: couldn't get cluster host name: {reason}".format(reason=e)
			exit(-1)

		serviceUrl = API_ROOT + SERVICES_URI.format(namespace=namespaceSpec["metadata"]["name"])
		rcUrl = API_ROOT + REPLICATION_CONTROLLERS_URI.format(namespace=namespaceSpec["metadata"]["name"])
		volumeUrl = API_ROOT + PERSISTENT_VOLUMES_URI.format(namespace=namespaceSpec["metadata"]["name"])

		# pipeline front-end service
		path = os.path.join(TEMPLATES_PATH, "pipeline-frontend-service.json.jinja2")  # TODO: rafactor paths
		with open(path) as f:
			pipelineFrontendServiceSpec = prepareTemplate(f)

		createResource(serviceUrl, API_HEADERS, pipelineFrontendServiceSpec)

		# sqlite-reader-service
		with open(os.path.join(TEMPLATES_PATH, "sqlite-reader-service.json.jinja2")) as f:
			sqliteReaderServiceSpec = prepareTemplate(f)

		createResource(serviceUrl, API_HEADERS, sqliteReaderServiceSpec)

		# sqlite-writer-service
		with open(os.path.join(TEMPLATES_PATH, "sqlite-writer-service.json.jinja2")) as f:
			sqliteWriterServiceSpec = prepareTemplate(f)

		createResource(serviceUrl, API_HEADERS, sqliteWriterServiceSpec)

		# rabbitmq service
		with open(os.path.join(TEMPLATES_PATH, "rabbitmq-service.json.jinja2")) as f:
			rabbitmqServiceSpec = prepareTemplate(f)

		createResource(serviceUrl, API_HEADERS, rabbitmqServiceSpec)

		# temporary config-writer-rc (with hostpath volume)
		with open(os.path.join(TEMPLATES_PATH, "config-writer-rc.json.jinja2")) as f:
			configWriterRcSpec = prepareTemplate(f)  # TODO: kwargs

		configWriterRcSpec["spec"]["template"]["spec"]["volumes"][0] = {
			"hostPath": {
				"path": "/var/lib/isb-cgc-pipelines/config"
			}
		}

		createResource(rcUrl, API_HEADERS, configWriterRcSpec)

		# temporary sqlite-writer-rc (with hostpath volume)
		with open(os.path.join(TEMPLATES_PATH, "sqlite-writer-rc.json.jinja2")) as f:
			sqliteWriterRcSpec = prepareTemplate(f)

		sqliteWriterRcSpec["spec"]["template"]["spec"]["volumes"][0] = {
			"hostPath": {
				"path": "/var/lib/isb-cgc-pipelines/db"
			}
		}

		sqliteWriterRcSpec["spec"]["template"]["spec"]["volumes"][1] = {
			"hostPath": {
				"path": "/var/lib/isb-cgc-pipelines/config"
			}
		}

		sqliteWriterRcSpec["spec"]["nodeName"] = node

		createResource(rcUrl, API_HEADERS, sqliteWriterRcSpec)

		# temporary sqlite-reader-rc (with hostpath volume)
		with open(os.path.join(TEMPLATES_PATH, "sqlite-reader-rc.json.jinja2")) as f:
			sqliteReaderRcSpec = prepareTemplate(f)

		sqliteReaderRcSpec["spec"]["template"]["spec"]["volumes"][0] = {
			"hostPath": {
				"path": "/var/lib/isb-cgc-pipelines/db"
			}
		}

		sqliteReaderRcSpec["spec"]["template"]["spec"]["volumes"][1] = {
			"hostPath": {
				"path": "/var/lib/isb-cgc-pipelines/config"
			}
		}

		sqliteReaderRcSpec["spec"]["nodeName"] = node

		createResource(rcUrl, API_HEADERS, sqliteReaderRcSpec)

		# temporary rabbitmq-rc (with hostpath volume)
		with open(os.path.join(TEMPLATES_PATH, "temporary-rabbitmq-rc.json.jinja2")) as f:
			rabbitmqRcSpec = prepareTemplate(f)  # TODO: kwargs

		rabbitmqRcSpec["spec"]["template"]["spec"]["volumes"][0] = {
			"hostPath": {
				"path": "/var/lib/isb-cgc-pipelines/rabbitmq"
			}
		}

		rabbitmqRcSpec["spec"]["nodeName"] = node

		createResource(rcUrl, API_HEADERS, rabbitmqRcSpec)

		# temporary pipeline frontend (with hostpath volume)
		with open(os.path.join(TEMPLATES_PATH, "pipeline-frontend-rc.json.jinja2")) as f:
			pipelineFrontendRcSpec = prepareTemplate(f, replicas=1)  # TODO: add replication factor to local config or cli

		pipelineFrontendRcSpec["spec"]["template"]["spec"]["volumes"][0] = {
			"hostPath": {
				"path": "/var/lib/isb-cgc-pipelines/config"
			}
		}

		pipelineFrontendRcSpec["spec"]["nodeName"] = node

		createResource(rcUrl, API_HEADERS, pipelineFrontendRcSpec)

		# temporary pipeline log handler rcs (with hostpath volume)
		with open(os.path.join(TEMPLATES_PATH, "pipeline-log-handler-rc.json.jinja2")) as f:
			pipelineLogHandlerRcSpec = prepareTemplate(f, replicas=1, subscription="pipelineVmInsert")

		pipelineLogHandlerRcSpec["spec"]["template"]["spec"]["volumes"][0] = {
			"hostPath": {
				"path": "/var/lib/isb-cgc-pipelines/config"
			}
		}

		pipelineLogHandlerRcSpec["spec"]["nodeName"] = node

		createResource(rcUrl, API_HEADERS, pipelineLogHandlerRcSpec)

		with open(os.path.join(TEMPLATES_PATH, "pipeline-log-handler-rc.json.jinja2")) as f:
			pipelineLogHandlerRcSpec = prepareTemplate(f, replicas=1, subscription="pipelineVmDelete")

		pipelineLogHandlerRcSpec["spec"]["template"]["spec"]["volumes"][0] = {
			"hostPath": {
				"path": "/var/lib/isb-cgc-pipelines/config"
			}
		}

		pipelineLogHandlerRcSpec["spec"]["nodeName"] = node

		createResource(rcUrl, API_HEADERS, pipelineLogHandlerRcSpec)

		with open(os.path.join(TEMPLATES_PATH, "pipeline-log-handler-rc.json.jinja2")) as f:
			pipelineLogHandlerRcSpec = prepareTemplate(f, replicas=1, subscription="pipelineVmPreempted")

		pipelineLogHandlerRcSpec["spec"]["template"]["spec"]["volumes"][0] = {
			"hostPath": {
				"path": "/var/lib/isb-cgc-pipelines/config"
			}
		}

		pipelineLogHandlerRcSpec["spec"]["nodeName"] = node

		createResource(rcUrl, API_HEADERS, pipelineLogHandlerRcSpec)

		# temporary pipeline scheduler rc (with hostpath volume)
		with open(os.path.join(TEMPLATES_PATH, "pipeline-scheduler-rc.json.jinja2")) as f:
			pipelineSchedulerRcSpec = prepareTemplate(f, replicas=1)

		pipelineSchedulerRcSpec["spec"]["template"]["spec"]["volumes"][0] = {
			"hostPath": {
				"path": "/var/lib/isb-cgc-pipelines/config"
			}
		}

		pipelineSchedulerRcSpec["spec"]["nodeName"] = node

		createResource(rcUrl, API_HEADERS, pipelineSchedulerRcSpec)

		# temporary pipeline canceller rc (with hostpath volume)
		with open(os.path.join(TEMPLATES_PATH, "pipeline-canceller-rc.json.jinja2")) as f:
			pipelineCancellerRcSpec = prepareTemplate(f, replicas=1)

		pipelineCancellerRcSpec["spec"]["template"]["spec"]["volumes"][0] = {
			"hostPath": {
				"path": "/var/lib/isb-cgc-pipelines/config"
			}
		}

		pipelineCancellerRcSpec["spec"]["nodeName"] = node

		createResource(rcUrl, API_HEADERS, pipelineCancellerRcSpec)

		# TODO: get the rabbitmq pod name
		rabbitmqPod = ""

		try:
			subprocess.check_call(["kubectl", "port-forward", rabbitmqPod, "5672:5672"])

		except subprocess.CalledProcessError as e:
			print "ERROR: couldn't port-forward the rabbitmq port: {reason}".format(reason=e)
			exit(-1)

		# set up the watch
		PipelineExchange('WATCH_EXCHANGE')

		# run pipeline jobs for creating/populating the service disks
		createDisks = PipelineBuilder(config)

		configReq = {
			"disk_name": "pipelines-service-config",  # TODO: make unique
			"disk_type": "PERSISTENT_SSD",
			"disk_size": 10,
			"disk_zone": zone
		}

		try:
			PipelineService.sendRequest()

		except PipelineServiceError as e:
			print "ERROR: Couldn't create the service volume (config): {reason}".format(reason=e)
			exit(-1)

		databaseReq = {
			"disk_name": "pipelines-service-db",  # TODO: make unique
			"disk_type": "PERSISTENT_SSD",
			"disk_size": 10,  # TODO: add to local config or cli
			"disk_zone": zone
		}

		try:
			PipelineService.sendRequest()

		except PipelineServiceError as e:
			print "ERROR: Couldn't create the service volume (database): {reason}".format(reason=e)
			exit(-1)

		msgReq = {
			"disk_name": "pipelines-service-msgs",  # TODO: make unique
			"disk_type": "PERSISTENT_SSD",
			"disk_size": 10,  # TODO: add to local config or cli
			"disk_zone": zone
		}

		try:
			PipelineService.sendRequest()

		except DataDiskError as e:
			print "ERROR: Couldn't create the service volume (msgs): {reason}".format(reason=e)
			exit(-1)

		# create a temporary bucket for log outputs
		u = uuid.uuid4()

		try:
			subprocess.check_call(["gsutil", "mb", "pipelines-tmp-{uuid}".format(uuid=u)])
		except subprocess.CalledProcessError as e:
			print "ERROR: couldn't create temporary bucket in GCS: {reason}".format(reason=e)
			exit(-1)

		# create and upload the configuration to the temporary bucket

		configDiskJob = PipelineSchema("bootstrap-config-disk", config,
		                               "gs://pipelines-tmp-{uuid}".format(uuid=u),
		                               "gcr.io/isb-cgc-public-docker-images/isb-cgc-pipelines",
		                               cmd="cd /etc/isb-cgc-pipelines && touch config && bootstrap-config --projectId {p}".format(p=projectId),
		                               cores=1,
		                               mem=2,
		                               zone=zone,
		                               tag=u)

		configDiskJob.addExistingDisk("pipelines-service-config", "PERSISTENT_SSD", projectId, zone, "/etc/isb-cgc-pipelines", autodelete=False)

		databaseDiskJob = PipelineSchema("bootstrap-db-disk", config,
		                               "gs://pipelines-tmp-{uuid}".format(uuid=u),
		                               "gcr.io/isb-cgc-public-docker-images/isb-cgc-pipelines",
		                               cmd="bootstrap-db".format(p=projectId),
		                               cores=1,
		                               mem=2,
		                               zone=zone,
		                               tag=u)

		databaseDiskJob.addExistingDisk("pipelines-service-db", "PERSISTENT_SSD", projectId, zone, "/var/lib/isb-cgc-pipelines", autodelete=False)

		messagingDiskJob = PipelineSchema("bootstrap-msg-disk", config,
		                                 "gs://pipelines-tmp-{uuid}".format(uuid=u),
		                                 "gcr.io/isb-cgc-public-docker-images/isb-cgc-pipelines",
		                                 cmd="ls",
		                                 cores=1,
		                                 mem=2,
		                                 zone=zone,
		                                 tag=u)

		databaseDiskJob.addExistingDisk("pipelines-service-msgs", "PERSISTENT_SSD", projectId, zone,
		                                "/var/lib/isb-cgc-pipelines", autodelete=False)

		createDisks.addStep(configDiskJob)
		createDisks.addStep(databaseDiskJob)
		createDisks.addStep(messagingDiskJob)

		try:
			jobIds = createDisks.run()

		except PipelineSchemaValidationError as e:
			print "ERROR: Couldn't bootstrap volume disks: {reason}".format(reason=e)
			exit(-1)

		except PipelineSubmitError as e:
			print "ERROR: Couldn't bootstrap volume disks: {reason}".format(reason=e)
			exit(-1)

		# set a watch on each job id and wait for the jobs to complete
		error = 0
		errors = []

		with ThreadPoolExecutor(2) as p:
			statuses = dict((p.submit(PipelineService.watchJob, j, 'WATCH_EXCHANGE') for j in jobIds))

			for s in as_completed(statuses):
				if s.exception() is not None:
					errors.append("ERROR: Couldn't create disk volume: {reason}".format(reason=s.exception()))
					error = 1

		if error == 1:
			for e in errors:
				print e

			exit(-1)

		# remove the temporary bucket and contents
		try:
			subprocess.check_call(["gsutil", "rm", "gs://pipelines-tmp-{uuid}/*".format(uuid=u)])
			subprocess.check_call(["gsutil", "rb", "gs://pipelines-tmp-{uuid}".format(uuid=u)])
		except subprocess.CalledProcessError as e:
			print "ERROR: couldn't cleanup temp bucket: {reason}".format(reason=e)

		# delete the temporary rcs
		deleteResource(rcUrl, API_HEADERS, configWriterRcSpec["metadata"]["name"])
		deleteResource(rcUrl, API_HEADERS, sqliteReaderRcSpec["metadata"]["name"])
		deleteResource(rcUrl, API_HEADERS, sqliteWriterRcSpec["metadata"]["name"])
		deleteResource(rcUrl, API_HEADERS, rabbitmqRcSpec["metadata"]["name"])

		# create the permanent volumes

		# permanent (pd) sqlite volume
		with open(os.path.join(TEMPLATES_PATH, "sqlite-volume.json.jinja2")) as f:
			sqliteVolume = prepareTemplate(f)  # TODO: kwargs

		createResource(volumeUrl, API_HEADERS, sqliteVolume)

		# permanent (pd) config volume
		with open(os.path.join(TEMPLATES_PATH, "config-volume.json.jinja2")) as f:
			configVolume = prepareTemplate(f)  # TODO: kwargs

		createResource(volumeUrl, API_HEADERS, configVolume)

		# permanent (pd) rabbitmq volume
		with open(os.path.join(TEMPLATES_PATH, "rabbitmq-volume.json.jinja2")) as f:
			rabbitmqVolume = prepareTemplate(f)  # TODO: kwargs

		createResource(volumeUrl, API_HEADERS, rabbitmqVolume)

		# restart the sqlite and rabbitmq RCs with the newly created volumes
		volume = {
			"name": "rabbitmq-data",
			"gcePersistentDisk": {
				"pdName": "{{ pdName }}",
				"readOnly": "false",
				"fsType": "ext4"
			}
		}

		# sqlite-reader rc
		with open(os.path.join(TEMPLATES_PATH, "sqlite-reader-rc.json.jinja2")) as f:
			sqliteReaderRcSpec = prepareTemplate(f, pdName="")  # TODO: pdName

		createResource(rcUrl, API_HEADERS, sqliteReaderRcSpec)

		# sqlite-writer rc
		with open(os.path.join(TEMPLATES_PATH, "sqlite-writer-rc.json.jinja2")) as f:
			sqliteWriterRcSpec = prepareTemplate(f, pdName="")  # TODO: pdName

		createResource(rcUrl, API_HEADERS, sqliteWriterRcSpec)

		# config-writer rc
		with open(os.path.join(TEMPLATES_PATH, "config-writer-rc.json.jinja2")) as f:
			configWriterRcSpec = prepareTemplate(f, pdName="")  # TODO: pdName

		createResource(rcUrl, API_HEADERS, configWriterRcSpec)

		# rabbitmq rc
		with open(os.path.join(TEMPLATES_PATH, "rabbitmq-rc.json.jinja2")) as f:
			rabbitmqRcSpec = prepareTemplate(f, pdName="")  # TODO: pdName

		createResource(rcUrl, API_HEADERS, rabbitmqRcSpec)

		# pipeline front-end rc
		with open(os.path.join(TEMPLATES_PATH, "pipeline-frontend-rc.json.jinja2")) as f:
			pipelineFrontendRcSpec = prepareTemplate(f, replicas=5)  # TODO: add replication factor to local config or cli

		createResource(rcUrl, API_HEADERS, pipelineFrontendRcSpec)

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