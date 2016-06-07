import os
import sys
import math
import string
import sqlite3
import requests
import pyinotify
import subprocess
from time import time, sleep
from datetime import datetime
from random import SystemRandom
from ConfigParser import SafeConfigParser

from googleapiclient.errors import HttpError

# Kubernetes API Access
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
	"Content-type": "application/json",
	"Authorization": "Bearer {access_token}"
}

class PipelinesConfig(object):
	def __init__(self, path=None):
		if path is not None:
			self.path = path
		else:
			self.path = os.path.join(os.environ["HOME"], ".isb-cgc-pipelines", "config")

		config = SafeConfigParser()
		requiredParams = {
			"gcp": ["project_id", "zones", "scopes", "service_account_email"],
			"pipelines": ["pipelines_home", "max_running_jobs", "autorestart_preempted", "polling_interval"]
		}

		innerDict = {}
		try:
			config.read(self.path)
		except IOError as e:
			print "Couldn't open ~/.isb-cgc-pipelines/config : {reason}".format(reason=e)
			exit(-1)
		else:
			for s, o in requiredParams.iteritems():
				if not config.has_section(s):
					print "ERROR: missing required section {s} in the configuration!\nRUN `isb-cgc-pipelines config` to correct the configuration".format(s=s)
					exit(-1)

				for option in o:
					if not config.has_option(s, option):
						print "ERROR: missing required option {o} in section {s}!\nRun `isb-cgc-pipelines config` to correct the configuration".format(s=s, o=option)
						exit(-1)
					innerDict[option] = config.get(s, option)
		
		self.__dict__.update(innerDict)

	def update(self, valuesDict):
		self.__dict__.update(valuesDict)
		
	def refresh(self):
		self.__init__(self.path)


class PipelinesConfigUpdateHandler(pyinotify.ProcessEvent):
	def my_init(self, config=None): # config -> PipelinesConfig
		self._config = config
	
	def process_IN_CLOSE_WRITE(self, event):
		PipelineSchedulerUtils.writeStdout("Refreshing configuration ...")
		self._config.refresh()


class PipelineSchedulerUtils(object):		

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


class PipelineDbUtils(object):
	def __init__(self, config):
		self._config = config
		self._sqliteConn = sqlite3.connect(os.path.join(self._config.pipelines_home, 'isb-cgc-pipelines.db'))
		self._pipelinesDb = self._sqliteConn.cursor()

	def closeConnection(self):
		self._sqliteConn.close()

	def insertJob(self, *args):  # *args: operationId, pipelineName, tag, currentStatus, preemptions, gcsLogPath, stdoutLog, stderrLog
		self._pipelinesDb.execute("INSERT INTO jobs (operation_id, pipeline_name, tag, current_status, preemptions, gcs_log_path, stdout_log, stderr_log, create_time, end_time, processing_time) VALUES (?,?,?,?,?,?,?,?,?,?,?)", tuple(args))
		self._sqliteConn.commit()

		return self._pipelinesDb.lastrowid

	def insertJobDependency(self, parentId, childId):
		self._pipelinesDb.execute("INSERT INTO job_dependencies (parent_id, child_id) VALUES (?,?)", (parentId, childId))
		self._sqliteConn.commit()

	def updateJob(self, jobId, setValues): # setValues -> dict
		if "preemptions" in setValues.keys():
			query = "UPDATE jobs SET preemptions = preemptions + 1 WHERE job_id = ?"
			self._pipelinesDb.execute(query, (jobId,))
			setValues.pop("preemptions")

		query = "UPDATE jobs SET {values} WHERE job_id = ?".format(values=','.join(["{v} = ?".format(v=v) for v in setValues.iterkeys()]))

		self._pipelinesDb.execute(query, tuple(setValues.itervalues()) + (jobId,))
		self._sqliteConn.commit()

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
				select = ["job_id", "operation_id", "pipeline_name", "tag", "current_status", "preemptions", "gcs_log_path", "stdout_log", "stderr_log", "create_time", "end_time", "processing_time"]

			for i, k in enumerate(select):
				newDict[k] = j[i]

			jobsList.append(JobInfo(newDict))

		return jobsList

	def createJobTables(self):
		if len(self._pipelinesDb.execute('SELECT name FROM sqlite_master WHERE type="table" AND name="jobs"').fetchall()) == 0:
			self._pipelinesDb.execute('CREATE TABLE jobs (job_id INTEGER PRIMARY KEY AUTOINCREMENT, operation_id VARCHAR(128), pipeline_name VARCHAR(128), tag VARCHAR(128), current_status VARCHAR(128), preemptions INTEGER, gcs_log_path VARCHAR(128), stdout_log VARCHAR(128), stderr_log VARCHAR(128), create_time VARCHAR(128), end_time VARCHAR(128), processing_time FLOAT)')
			self._sqliteConn.commit()

		if len(self._pipelinesDb.execute('SELECT name FROM sqlite_master WHERE type="table" AND name="job_dependencies"').fetchall()) == 0:
			self._pipelinesDb.execute("CREATE TABLE job_dependencies (row_id INTEGER PRIMARY KEY AUTOINCREMENT, parent_id INTEGER, child_id INTEGER)")
			self._sqliteConn.commit()


class PipelineServiceUtils:

	# TODO: eventually refactor this class
	@staticmethod
	def bootstrapCluster(compute, gke, http, config):
		# create a cluster to run the workflow on if it doesn't exist already

		def createClusterAdminPassword():
			return ''.join(
				SystemRandom().choice(string.ascii_uppercase + string.ascii_lowercase + string.digits) for _ in
				range(16))

		def clusterExists(gke, http, config):
			clusterExists = False
			clusterEndpoint = None

			try:
				response = gke.projects().zones().clusters().get(projectId=config.project_id, zone=config.service_zone,
				                                                 clusterId=config.service_name).execute(
					http=http)
			except HttpError:
				pass
			else:
				# make sure the existing cluster meets the requirements in the global configuration
				try:
					if str(response["name"]) == config.service_name and str(
							response["initialNodeCount"]) == config.service_node_count and str(
						response["nodeConfig"]["diskSizeGb"]) == config.service_disk_size and str(
						response["nodeConfig"]["machineType"]) == config.service_machine_type:
						clusterExists = True
						clusterEndpoint = response["endpoint"]

				except KeyError:
					pass

			return clusterExists, clusterEndpoint

		cluster = {
			"cluster": {
				"name": "{cluster_name}".format(cluster_name=config.service_name),
				"zone": "{zone}".format(zone=config.service_zone),
				"initialNodeCount": config.node_count,
				"network": "{network}".format(network=config.service_network),
				"nodeConfig": {
					"machineType": "{machine_type}".format(machine_type=config.service_machine_type),
					"diskSizeGb": config.service_boot_disk_size,
					"oauthScopes": [
						"https://www.googleapis.com/auth/pubsub",
						"https://www.googleapis.com/auth/devstorage.read_write",
						"https://www.googleapis.com/auth/logging.write"
					]
				},
				"masterAuth": {
					"username": "admin",
					"password": "{password}".format(password=createClusterAdminPassword()),
				}
			}
		}

		exists, endpoint = clusterExists(gke, http, config)
		if not exists:
			print "Creating cluster {cluster} ... ".format(cluster=config.service_name)

			createCluster = gke.projects().zones().clusters().create(projectId=config.project_id, zone=config.service_zone,
			                                                          body=cluster).execute(http=http)

			# wait for the operation to complete
			while True:
				response = gke.projects().zones().operations().get(projectId=config.project_id, zone=config.service_zone,
				                                                   operationId=createCluster["name"]).execute(http=http)

				if response['status'] == 'DONE':
					break
				else:
					sleep(1)

		# configure cluster access (may want to perform checks before doing this)
		print "Configuring access to cluster {cluster_name} ...".format(cluster_name=config.service_name)

		# configure ssh keys
		try:
			subprocess.check_call(
				["bash", "-c", "stat ~/.ssh/google_compute_engine && stat ~/.ssh/google_compute_engine.pub"])
		except subprocess.CalledProcessError as e:
			try:
				subprocess.check_call(["gcloud", "compute", "config-ssh"])
			except subprocess.CalledProcessError as e:
				print "Couldn't generate SSH keys for the workstation: {e}".format(e=e)
				exit(-1)

		try:
			subprocess.check_call(["gcloud", "config", "set", "compute/zone", ",".join(config.zones)])
		except subprocess.CalledProcessError as e:
			print "Couldn't set the compute zone: {reason}".format(reason=e)
			exit(-1)
		try:
			subprocess.check_call(["kubectl", "config", "set", "cluster", config.service_name])
		except subprocess.CalledProcessError as e:
			print "Couldn't set cluster in configuration: {reason}".format(reason=e)
			exit(-1)  # raise an exception

		try:
			subprocess.check_call(["gcloud", "container", "clusters", "get-credentials", config.service_name])
		except subprocess.CalledProcessError as e:
			print "Couldn't get cluster credentials: {reason}".format(reason=e)
			exit(-1)  # raise an exception

		# get the name of a host to use for formatting the data disk
		try:
			instanceGroupName = subprocess.check_output(["gcloud", "compute", "instance-groups", "list", "--regexp", "^gke-{service}-.*$".format(service=config.service_name)]).split('\n')[1].split(' ')[0]
			instance_list = subprocess.check_output(["gcloud", "compute", "instance-groups", "list-instances", instanceGroupName]).split('\n')[1:-1]
			formattingInstance = instance_list[0]

		except subprocess.CalledProcessError as e:
			print "Couldn't get cluster hostnames: {reason}".format(reason=e)
			exit(-1)  # raise an exception

		# run kubectl in proxy mode in a background process
		try:
			subprocess.Popen(["kubectl", "proxy", "--port", "8080"])
		except ValueError as e:
			exit(-1)  # raise an exception

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
				print "Couldn't access proxy (timeout reached): {status}".format(status=response.content)
				exit(-1)
			else:
				sleep(1)
				timeout -= 1

		# create a namespace for the service
		namespace = {
			"apiVersion": "v1",
			"kind": "Namespace",
			"metadata": {
				"name": config.service_name
			}
		}

		fullUrl = API_ROOT + NAMESPACE_URI
		response = SESSION.post(fullUrl, headers=API_HEADERS, json=namespace)

		# if the response isn't what's expected, raise an exception
		if response.status_code != 201:
			if response.status_code == 409:  # already exists
				pass
			else:
				print "Namespace creation failed: {reason}".format(reason=response.status_code)
				exit(-1)  # probably should raise an exception

		# set the namespace for the current context if it doesn't exist already
		kubectlConfig = subprocess.Popen(["kubectl", "config", "view"], stdout=subprocess.PIPE)
		grep = subprocess.Popen(["grep", "current-context"], stdout=subprocess.PIPE, stdin=kubectlConfig.stdout,
		                        stderr=subprocess.STDOUT)

		kubectlContextString = grep.communicate()
		kubeContext = kubectlContextString[0].split(' ')[1].strip()
		try:
			subprocess.check_call(["kubectl", "config", "set", "contexts.{context}.namespace".format(context=kubeContext), config.service_name])
		except subprocess.CalledProcessError as e:
			print"Couldn't set cluster context: {reason}".format(reason=e)
			exit(-1)  # raise an exception

		# Create and format a data disk for the service containers

		disk = {
			"name": "{service}-data".format(service=config.service_name),
			"zone": "https://www.googleapis.com/compute/v1/projects/{project}/zones/{zone}".format(project=config.project_id, zone=config.service_zone),
			"type": "https://www.googleapis.com/compute/v1/projects/{project}/zones/{zone}/diskTypes/pd-ssd".format(project=config.project_id, zone=config.service_zone),
			"sizeGb": config.service_disk_size
		}

		# Submit the disk request
		diskResponse = compute.disks().insert(project=config.project_id, zone=config.service_zone, body=disk).execute()

		# Wait for the disks to be created
		while True:
			try:
				result = compute.zoneOperations().get(project=config.project_id, zone=config.service_zone,
				                                      operation=diskResponse['name']).execute()
			except HttpError:
				break
			else:
				if result['status'] == 'DONE':
					break

		# attach the disk to an instance for formatting
		attachRequest = {
			"kind": "compute#attachedDisk",
			"index": 1,
			"type": "PERSISTENT",
			"mode": "READ_WRITE",
			"source": "https://www.googleapis.com/compute/v1/projects/{project}/zones/{zone}/disks/{disk_name}".format(
				project=config.project_id, zone=config.service_zone, disk_name=disk["name"]),
			"deviceName": disk["name"],
			"boot": False,
			"interface": "SCSI",
			"autoDelete": False
		}

		success = False
		attachResponse = compute.instances().attachDisk(project=config.project_id, zone=config.service_zone, instance=formattingInstance,
		                                                 body=attachRequest).execute()

		# Wait for the attach operation to complete
		while True:
			try:
				result = compute.zoneOperations().get(project=config.project_id, zone=config.service_zone,
				                                      operation=attachResponse['name']).execute()
			except HttpError:
				break
			else:
				if result['status'] == 'DONE':
					success = True
					break

			if success:
				break

			else:
				print "Couldn't attach disk for formatting: {result}".format(result=result)
				exit(-1)

		# generate ssh keys on the workstation
		try:
			subprocess.check_call(
				["bash", "-c", "stat ~/.ssh/google_compute_engine && stat ~/.ssh/google_compute_engine.pub"])
		except subprocess.CalledProcessError as e:
			try:
				subprocess.check_call(["gcloud", "compute", "config-ssh"])
			except subprocess.CalledProcessError as e:
				print "Couldn't generate SSH keys for the workstation: {e}".format(e=e)
				exit(-1)

		command = (
			"sudo mkdir -p /{service}-data && "
			"sudo mount -o discard,defaults /dev/disk/by-id/google-{service}-data /{service}-data && "
			"sudo mkfs.ext4 -F /dev/disk/by-id/google-{service}-data && "
			"sudo umount /dev/disk/by-id/google-{service}-data"
		).format(service=config.service_name)
		try:
			subprocess.check_call(["gcloud", "compute", "ssh", formattingInstance, "--command", command])
		except subprocess.CalledProcessError as e:
			print "Couldn't format the disk: {e}".format(e=e)
			exit(-1)

		detachResponse = compute.instances().detachDisk(project=config.project_id, zone=config.service_zone, instance=formattingInstance,
		                                                 deviceName=config.service_name).execute()

		# Wait for the detach operation to complete
		while True:
			try:
				result = compute.zoneOperations().get(project=config.project_id, zone=config.service_zone,
				                                      operation=detachResponse['name']).execute()
			except HttpError:
				break
			else:
				if result['status'] == 'DONE':
					break

		# create the RCs and Services
		pipelineVolume = {

		}

		pipelineVolumeClaim = {

		}

		mysqlReaderRc = {

		}

		mysqlReaderService = {

		}

		mysqlWriterRc = {

		}

		mysqlWriterService = {

		}

		pipelineRc = {
			"apiVersion": "v1",
			"kind": "ReplicationController",
			"metadata": {
				"name": "pipeline-server"
			},
			"spec": {
				"replicas": config.service_container_replicas,
				"selector": {
					"role": "pipeline-server"
				},
				"template": {
					"metadata": {
						"labels": {
							"role": "pipeline-server"
						}
					},
					"spec": {
						"containers": [
							{
								"name": "pipeline-server",
								"image": "b.gcr.io/isb-cgc-pipelines-public-docker-images/isb-cgc-pipelines:latest",
								"ports": [
									{
										"name": "pipelines",
										"containerPort": 80
									}
								],
								"securityContext": {
									"privileged": True
								}
							}
						]
					}
				}
			}

		}

		pipelineService = {
			"kind": "Service",
			"apiVersion": "v1",
			"metadata": {
				"name": "pipeline-server"
			},
			"spec": {
				"ports": [
					{
						"port": 80
					}
				],
				"selector": {
			    		"role": "pipeline-server"
				}
			}
		}

		print "Cluster bootstrap was successful!"

	@staticmethod
	def bootstrapFunctions(functions, pubsub, logging, http, config):
		##
		## create the following pubsub topics (according to configuration values):
		## pipeline-server-logs (not required), pipeline-job-stdout-logs (not required), pipeline-job-stderr-logs (not required),
		## pipeline-vm-insert (required), pipeline-vm-preempted (required), pipeline-vm-delete (required)
		##
		## deploy functions corresponding to the created pubsub topics
		##
		## create log sinks for pipeline vm logs
		pass

class DataUtils(object):
	@staticmethod
	def getAnalysisDetail(analysisId):
		cghubMetadataUrl = "https://cghub.ucsc.edu/cghub/metadata/analysisDetail?analysis_id={analysisId}"

		headers = {
			"Accept": "application/json"
		}

		return requests.get(cghubMetadataUrl.format(analysisId=analysisId), headers=headers).json()

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

