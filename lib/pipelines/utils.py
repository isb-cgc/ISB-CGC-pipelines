import os
import sys
import pika
import math
import string
import sqlite3
import requests
import pyinotify
import subprocess
from time import time, sleep
from datetime import datetime
from random import SystemRandom
from ConfigParser import SafeConfigParser, NoOptionError, NoSectionError

from googleapiclient.errors import HttpError

MODULE_PATH = "/usr/local/ISB-CGC-pipelines/lib"  # TODO: move path to configuration file

# Kubernetes API Access
API_ROOT = "http://localhost:8080"
NAMESPACE_URI = "/api/v1/namespaces/"
PODS_URI = "/api/v1/namespaces/{namespace}/pods/"
SERVICES_URI = "/api/v1/namespaces/{namespace}/services/"
REPLICATION_CONTROLLERS_URI = "/api/v1/namespaces/{namespace}/replicationcontrollers/"
PERSISTENT_VOLUMES_URI = "/api/v1/persistentvolumes/"
PERSISTENT_VOLUME_CLAIMS_URI = "/api/v1/namespaces/{namespace}/persistentvolumeclaims/"
SECRETS_URI = "/api/v1/namespaces/{namespace}/secrets/"

K8S_SESSION = requests.Session()
API_HEADERS = {
	"Content-type": "application/json",
	"Authorization": "Bearer {access_token}"
}


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
				"default": "https://www.googleapis.com/auth/pubsub,https://www.googleapis.com/auth/compute,https://www.googleapis.com/auth/devstorage.full_control",
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
				"default": 200,
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
			},
			"service_name": {
				"section": "service",
				"required": False,
				"default": None,
				"message": "Enter a name for the service deployment: "
			},
			"service_zone": {
				"section": "service",
				"required": False,
				"default": "us-central1-a",
				"message": "Enter a zone for the service deployment (leave blank to use default us-central1-a): "
			},
			"service_boot_disk_size": {
				"section": "service",
				"required": False,
				"default": 100,
				"message": "Enter the boot disk size for the service cluster nodes, in GB (leave blank to use default 100): "
			},
			"service_network": {
				"section": "service",
				"required": False,
				"default": "default",
				"message": "Enter the network to use for the service deployment (leave blank to use the 'default' network): "
			},
			"service_endpoint": {
				"section": "service",
				"required": False,
				"default": None,
				"message": None  # this should only be set once the service has been deployed
			},
			"service_node_count": {
				"section": "service",
				"required": False,
				"default": 1,
				"message": "Enter the number of nodes to use for the service cluster (leave blank to use the default of one node): "
			},
			"service_cores": {
				"section": "service",
				"required": False,
				"default": "4",
				"message": "Enter the number of cores per instance to use for the service cluster (leave blank to use the default 1): "
			},
			"service_mem": {
				"section": "service",
				"required": False,
				"default": "2",
				"message": "Enter the amount of RAM per instance (in GB) to use for the service cluster (leave blank to use the default 2): "
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
		rabbitmqConn.close()

		pipelineDbUtils = PipelineDbUtils(config)
		pipelineDbUtils.createJobTables()
		#pipelineDbUtils.closeConnection()

		c = SafeConfigParser()
		
		try:
			c.readfp(open("/etc/supervisor/supervisord.conf"))
		except IOError as e:
			print "ERROR: supervisor config file (/etc/supervisor/supervisord.conf) -- double check your supervisor installation."
			exit(-1)
		else:
			if not c.has_section("program:pipelineJobScheduler"):
				c.add_section("program:pipelineJobScheduler")

			if not c.has_section("program:pipelinePreemptedLogsHandler"):
				c.add_section("program:pipelinePreemptedLogsHandler")
				
			if not c.has_section("program:pipelineDeleteLogsHandler"):
				c.add_section("program:pipelineDeleteLogsHandler")

			c.set("program:pipelineJobScheduler", "process_name", "pipelineJobScheduler")
			c.set("program:pipelineJobScheduler", "command",
			      "pipelineJobScheduler --config {config}".format(config=config.path))
			c.set("program:pipelineJobScheduler", "environment",
			      "PYTHONPATH={modulePath}".format(modulePath=MODULE_PATH))
			c.set("program:pipelineJobScheduler", "numprocs", "1")
			c.set("program:pipelineJobScheduler", "autostart", "true")
			c.set("program:pipelineJobScheduler", "autorestart", "true")
			c.set("program:pipelineJobScheduler", "user", user)

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

			with open("/etc/supervisor/supervisord.conf", "w") as f:
				c.write(f)

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
	def __init__(self, config):
		self._connection = pika.BlockingConnection(pika.ConnectionParameters(host='localhost'))
		self._channel = self._connection.channel()
		self._channel.queue_declare(queue='WAIT_Q', durable=True)

	def __del__(self):
		self._connection.close()

	def publish(self, msg):
		self._channel.basic_publish(exchange='', routing_key="WAIT_Q", body=msg)

	def get(self):
		self._channel.basic_qos(prefetch_count=1)
		method, header, body = self._channel.basic_get('WAIT_Q')

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
		self._pipelinesDb.execute("INSERT INTO jobs (operation_id, pipeline_name, tag, current_status, preemptions, gcs_log_path, stdout_log, stderr_log, create_time, end_time, processing_time, request) VALUES (?,?,?,?,?,?,?,?,?,?,?,?)", tuple(args))
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

	# TODO: eventually refactor this class
	# Realistically, the methods in this class ought to be idempotent -- may consider using a configuration management tool instead

	@staticmethod
	def bootstrapCluster(compute, gke, http, config):
		# create a cluster to run the workflow on if it doesn't exist already

		def createPassword():
			return ''.join(
				SystemRandom().choice(string.ascii_uppercase + string.ascii_lowercase + string.digits) for _ in
				range(16))

		def clusterExists(gke, http, config):
			clusterExists = False
			clusterEndpoint = None

			try:
				response = gke.projects().zones().clusters().get(projectId=config.project_id, zone=config.service_zone, clusterId=config.service_name).execute(http=http)
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
					"machineType": "custom-{cores}-{mib}".format(cores=config.service_cores, mib=str(int(config.service_mem)*1024)),
					"diskSizeGb": config.service_boot_disk_size,
					"oauthScopes": [
						"https://www.googleapis.com/auth/pubsub",
						"https://www.googleapis.com/auth/devstorage.read_write",
						"https://www.googleapis.com/auth/logging.write"
					]
				},
				"masterAuth": {
					"username": "admin",
					"password": "{password}".format(password=createPassword()),
				}
			}
		}

		exists, clusterEndpoint = clusterExists(gke, http, config)
		if not exists:
			print "Creating cluster {cluster} ... ".format(cluster=config.service_name)

			createCluster = gke.projects().zones().clusters().create(projectId=config.project_id, zone=config.service_zone, body=cluster).execute(http=http)
			clusterEndpoint = createCluster["cluster"]["endpoint"]

			# wait for the operation to complete
			while True:
				response = gke.projects().zones().operations().get(projectId=config.project_id, zone=config.service_zone, operationId=createCluster["name"]).execute(http=http)
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

		# set the cluster endpoint in the configuration
		config.update("service", "service_cluster_endpoint", clusterEndpoint)

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
				response = K8S_SESSION.get(API_ROOT + NAMESPACE_URI)
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
		response = K8S_SESSION.post(fullUrl, headers=API_HEADERS, json=namespace)

		# if the response isn't what's expected, raise an exception
		if response.status_code != 201:
			if response.status_code == 409:  # already exists
				pass
			else:
				print "Namespace creation failed: {reason}".format(reason=response.status_code)
				exit(-1)  # probably should raise an exception

		# set the namespace for the current context if it doesn't exist already
		kubectlConfig = subprocess.Popen(["kubectl", "config", "view"], stdout=subprocess.PIPE)
		grep = subprocess.Popen(["grep", "current-context"], stdout=subprocess.PIPE, stdin=kubectlConfig.stdout, stderr=subprocess.STDOUT)

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
				result = compute.zoneOperations().get(project=config.project_id, zone=config.service_zone, operation=diskResponse['name']).execute()
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
		attachResponse = compute.instances().attachDisk(project=config.project_id, zone=config.service_zone, instance=formattingInstance, body=attachRequest).execute()

		# Wait for the attach operation to complete
		while True:
			try:
				result = compute.zoneOperations().get(project=config.project_id, zone=config.service_zone, operation=attachResponse['name']).execute()
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

		detachResponse = compute.instances().detachDisk(project=config.project_id, zone=config.service_zone, instance=formattingInstance, deviceName=config.service_name).execute()

		# Wait for the detach operation to complete
		while True:
			try:
				result = compute.zoneOperations().get(project=config.project_id, zone=config.service_zone, operation=detachResponse['name']).execute()
			except HttpError:
				break
			else:
				if result['status'] == 'DONE':
					break

		# create the service account secret (?)


		# create the RCs and Services
		mysqlPass = createPassword()
		rcUrl = API_ROOT + REPLICATION_CONTROLLERS_URI
		serviceUrl = API_ROOT + SERVICES_URI

		mysqlWriterRc = {
			"apiVersion": "v1",
			"kind": "ReplicationController",
			"metadata": {
				"name": "mysql-writer"
			},
			"spec": {
				"replicas": 1,
				"selector": {
					"role": "mysql-writer"
				},
				"template": {
					"metadata": {
						"labels": {
							"role": "mysql-writer"
						}
					},
					"spec": {
						"containers": [
							{
								"name": "mysql-writer",
								"image": "mysql",
								"env": [
									{
										"name": "MYSQL_ROOT_PASSWORD",
										"value": mysqlPass
									},
									{
										"name": "MYSQL_DATABASE",
										"value": "pipelines"
									}
								],
								"ports": [
									{
										"name": "mysql-port",
										"containerPort": 3306
									}
								],
								"securityContext": {
									"privileged": True
								},
								"volumeMounts": [
									{
										"name": "pipeline-job-data",
										"mountPath": "/var/lib/mysql",
										"readOnly": False
									}
								]
							}
						],
						"volumes": [
							{
								"name": "pipeline-job-data",
								"gcePersistentDisk": {
									"pdName": diskResponse["name"],
									"readOnly": False,
									"fsType": "ext4"
								}
							}
						]
					}
				}
			}
		}

		response = K8S_SESSION.post(rcUrl, headers=API_HEADERS, json=mysqlWriterRc)

		# if the response isn't what's expected, raise an exception
		if response.status_code != 201:
			if response.status_code == 409:  # already exists
				pass
			else:
				print "MySQL writer creation failed: {reason}".format(reason=response.status_code)
				exit(-1)  # probably should raise an exception

		mysqlWriterService = {
			"kind": "Service",
			"apiVersion": "v1",
			"metadata": {
				"name": "mysql-server"
			},
			"spec": {
				"ports": [
					{
						"port": 3306
					}
				],
				"selector": {
					"role": "mysql-server"
				}
			}
		}

		response = K8S_SESSION.post(serviceUrl, headers=API_HEADERS, json=mysqlWriterService)

		# if the response isn't what's expected, raise an exception
		if response.status_code != 201:
			if response.status_code == 409:  # already exists
				pass
			else:
				print "MySQL writer service creation failed: {reason}".format(reason=response.status_code)
				exit(-1)  # probably should raise an exception

		mysqlReaderRc = {
			"apiVersion": "v1",
			"kind": "ReplicationController",
			"metadata": {
				"name": "mysql-reader"
			},
			"spec": {
				"replicas": int(config.service_cores) * int(config.service_node_count)/2,
				"selector": {
					"role": "mysql-reader"
				},
				"template": {
					"metadata": {
						"labels": {
							"role": "mysql-reader"
						}
					},
					"spec": {
						"containers": [
							{
								"name": "mysql-reader",
								"image": "mysql",
								"env": [
									{
										"name": "MYSQL_ROOT_PASSWORD",
										"value": mysqlPass
									},
									{
										"name": "MYSQL_DATABASE",
										"value": "pipelines"
									}
								],
								"ports": [
									{
										"name": "mysql-port2",
										"containerPort": 3307
									}
								],
								"securityContext": {
									"privileged": True
								},
								"volumeMounts": [
									{
										"name": "pipeline-job-data",
										"mountPath": "/var/lib/mysql",
										"readOnly": True
									}
								]
							}
						],
						"volumes": [
							{
								"name": "pipeline-job-data",
								"gcePersistentDisk": {
									"pdName": diskResponse["name"],
									"readOnly": True,
									"fsType": "ext4"
								}
							}
						]
					}
				}
			}
		}

		response = K8S_SESSION.post(rcUrl, headers=API_HEADERS, json=mysqlWriterRc)

		# if the response isn't what's expected, raise an exception
		if response.status_code != 201:
			if response.status_code == 409:  # already exists
				pass
			else:
				print "MySQL writer creation failed: {reason}".format(reason=response.status_code)
				exit(-1)  # probably should raise an exception

		mysqlReaderService = {
			"kind": "Service",
			"apiVersion": "v1",
			"metadata": {
				"name": "mysql-reader"
			},
			"spec": {
				"ports": [
					{
						"port": 3307
					}
				],
				"selector": {
					"role": "mysql-reader"
				}
			}
		}

		response = K8S_SESSION.post(serviceUrl, headers=API_HEADERS, json=mysqlReaderService)

		# if the response isn't what's expected, raise an exception
		if response.status_code != 201:
			if response.status_code == 409:  # already exists
				pass
			else:
				print "MySQL reader service creation failed: {reason}".format(reason=response.status_code)
				exit(-1)  # probably should raise an exception

		pipelineRc = {
			"apiVersion": "v1",
			"kind": "ReplicationController",
			"metadata": {
				"name": "pipeline-server"
			},
			"spec": {
				"replicas": int(config.service_cores) * int(config.service_node_count)/2,
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
										"name": "pipeline-port",
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

		response = K8S_SESSION.post(rcUrl, headers=API_HEADERS, json=pipelineRc)

		# if the response isn't what's expected, raise an exception
		if response.status_code != 201:
			if response.status_code == 409:  # already exists
				pass
			else:
				print "Pipeline rc creation failed: {reason}".format(reason=response.status_code)
				exit(-1)

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
				},
				"type": "LoadBalancer"
			}
		}

		response = K8S_SESSION.post(serviceUrl, headers=API_HEADERS, json=pipelineService)

		# if the response isn't what's expected, exit
		if response.status_code != 201:
			if response.status_code == 409:  # already exists
				pass
			else:
				print "Pipeline service creation failed: {reason}".format(reason=response.status_code)
				exit(-1)

		# TODO: open firewall for port 80 on the cluster instances (for now, do this manually)

		# initialize the pipelines db
		PipelineDbUtils('root', 'pipelines', mysqlPass, config.service_endpoint, 3307).createJobTables()

		print "Cluster bootstrap was successful!"

	@staticmethod
	def bootstrapMessageHandlers(pubsub, logging, config, mode="local"):  # TODO: move topics that aren't required to the configuration file
		# create log sinks for pipeline vm logs
		timestamp = datetime.utcnow().isoformat("T") + "Z"  # RFC3339 timestamp

		topics = {
			"pipelineVmInsert": {
				"filter": ('resource.type="gce_instance" AND '
						'timestamp > {tz} AND jsonPayload.resource.name:"ggp-" AND '
						'jsonPayload.event_subtype="compute.instances.insert" AND '
						'NOT error AND logName="projects/{project}/logs/compute.googleapis.com%2Factivity_log"'
				).format(project=config.project_id, tz=timestamp),
				"trigger": "topic"
			},
			"pipelineVmPreempted": {
				"filter": ('resource.type="gce_instance" AND '
						'timestamp > {tz} AND jsonPayload.resource.name:"ggp-" AND '
						'jsonPayload.event_subtype="compute.instances.delete" AND '
						'NOT error AND logName="projects/{project}/logs/compute.googleapis.com%2Factivity_log"'
				).format(project=config.project_id, tz=timestamp),
				"trigger": "topic"
			},
			"pipelineVmDelete": {
				"filter": ('resource.type="gce_instance" AND '
						'timestamp > {tz} AND jsonPayload.resource.name:"ggp-" AND '
						'jsonPayload.event_subtype="compute.instances.preempted" AND '
						'NOT error AND logName="projects/{project}/logs/compute.googleapis.com%2Factivity_log"'
				).format(project=config.project_id, tz=timestamp),
				"trigger": "topic",
			}
			#"pipelineServerLogs": {  # TODO: possibly implement additional log collection
			#	"filter": "",
			#	"trigger": "http"
			#},
			#"pipelineJobStdoutLogs": {
			#	"filter": "",
			#	"trigger": "http"
			#},
			#"pipelineJobStderrLogs": {
			#	"filter": "",
			#	"trigger": "http"
			#}
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
				pubsub.subscriptions().get(subscription=subscription).execute()
			except HttpError:
				body = {
					"topic": topic,
					"name": subscription
				}
				try:
					pubsub.subscriptions.create(name=subscription, body=body)
				except HttpError as e:
					print "ERROR: couldn't create pubsub subscription {s}: {reason}".format(s=subscription, reaosn=e)
					exit(-1)

			body = {
				"destination": "googleapis.com/auth/pubsub/projects/{project}/topics/{t}".format(project=config.project_id, t=t),
				"filter": v["filter"],
				"name": t
			}

			sink = "projects/{project}/sinks/{t}".format(project=config.project_id, t=t)
			try:
				logging.projects().sinks().get(sinkName=sink)
			except HttpError as e:
				try:
					logging.projects().sinks().create(projectName="projects/{project}".format(project=config.project_id), body=body).execute()
				except HttpError as e:
					print "ERROR: couldn't create the pipelineVmInsert log sink : {reason}".format(reason=e)
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

