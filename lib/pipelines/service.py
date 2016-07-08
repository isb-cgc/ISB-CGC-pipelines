import os
import json
import subprocess
import string
from random import SystemRandom
from datetime import datetime
from jinja2 import Template
from googleapiclient.errors import HttpError
from pipelines.paths import *
from pipelines.db import PipelineDatabase, PipelineDatabaseError
from ConfigParser import SafeConfigParser


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
	def bootstrapServiceCluster(gke, projectId, zone, clusterName, machineType, nodes, nodeDiskSize, nodeDiskType):
		# TODO: need to pass in project_id, zone, cluster_name, node count,
		def create_cluster_admin_password(self):
			return ''.join(SystemRandom().choice(string.ascii_uppercase + string.ascii_lowercase + string.digits) for _ in range(16))

		def clusterExists():
			clusterExists = False

			# load and render the cluster template for request
			with open(os.path.join(TEMPLATES_PATH, "cluster.json.jinja2")) as f:
				s = f.read().replace('\n', '')

			t = Template(s)

			clusterSpec = json.loads(t.render(projectId=projectId, zone=zone, clusterName=clusterName, nodes=nodes, nodeDiskSize=nodeDiskSize, nodeDiskType=nodeDiskType))

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


						# TODO: set cluster endpoint in the client configuration

						cluster_exists = True
				except KeyError:
					pass

			return clusterExists

		# TODO: start here
		def ensureCluster(self, filestore):
			# create a cluster to run the workflow on if it doesn't exist already
			if not self.cluster_exists(filestore):
				filestore.logToMaster("{timestamp}  Creating cluster ... ".format(timestamp=self.create_timestamp()))
				create_cluster = GKE.projects().zones().clusters().create(projectId=self.project_id, zone=self.zone,
				                                                          body=self.cluster_spec).execute(http=HTTP)

				# wait for the operation to complete
				while True:
					response = GKE.projects().zones().operations().get(projectId=self.project_id, zone=self.zone,
					                                                   operationId=create_cluster["name"]).execute(
						http=HTTP)

					if response['status'] == 'DONE':
						break
					else:
						time.sleep(1)

		def configureClusterAccess(self, filestore):
			# configure cluster access (may want to perform checks before doing this)
			filestore.logToMaster("{timestamp}  Configuring access to cluster {cluster_name} ...".format(
				timestamp=self.create_timestamp(), cluster_name=self.workflow_name))

			# configure ssh keys
			try:
				subprocess.check_call(
					["bash", "-c", "stat ~/.ssh/google_compute_engine && stat ~/.ssh/google_compute_engine.pub"])
			except subprocess.CalledProcessError as e:
				try:
					subprocess.check_call(["gcloud", "compute", "config-ssh"])
				except subprocess.CalledProcessError as e:
					filestore.logToMaster("Couldn't generate SSH keys for the workstation: {e}".format(e=e))
					exit(-1)

			try:
				subprocess.check_call(["gcloud", "config", "set", "compute/zone", self.zone])
			except subprocess.CalledProcessError as e:
				filestore.logToMaster("Couldn't set the compute zone: {reason}".format(reason=e))
				exit(-1)
			try:
				subprocess.check_call(["kubectl", "config", "set", "cluster", self.workflow_name])
			except subprocess.CalledProcessError as e:
				filestore.logToMaster("Couldn't set cluster in configuration: {reason}".format(reason=e))
				exit(-1)  # raise an exception

			try:
				subprocess.check_call(["gcloud", "container", "clusters", "get-credentials", self.workflow_name])
			except subprocess.CalledProcessError as e:
				filestore.logToMaster("Couldn't get cluster credentials: {reason}".format(reason=e))
				exit(-1)  # raise an exception

			# get the cluster hosts for reference
			try:
				instance_group_name = subprocess.check_output(
					["gcloud", "compute", "instance-groups", "list", "--regexp",
					 "^gke-{workflow}-.*-group$".format(workflow=self.workflow_name)]).split('\n')[1].split(' ')[0]
				instance_list = subprocess.check_output(
					["gcloud", "compute", "instance-groups", "list-instances", instance_group_name]).split('\n')[1:-1]
				for instance in instance_list:
					self.cluster_hosts.append(instance.split(' ')[0])

			except subprocess.CalledProcessError as e:
				filestore.logToMaster("Couldn't get cluster hostnames: {reason}".format(reason=e))
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
					filestore.logToMaster(
						"Couldn't access proxy (timeout reached): {status}".format(status=response.content))
					exit(-1)
				else:
					time.sleep(1)
					timeout -= 1

			# create a namespace for the workflow
			full_url = API_ROOT + NAMESPACE_URI
			response = SESSION.post(full_url, headers=self.headers, json=self.namespace_spec)

			# if the response isn't what's expected, raise an exception
			if response.status_code != 201:
				if response.status_code == 409:  # already exists
					pass
				else:
					filestore.logToMaster("Namespace creation failed: {reason}".format(reason=response.status_code))
					exit(-1)  # probably should raise an exception

			# set the namespace for the current context if it doesn't exist already
			kubectl_config = subprocess.Popen(["kubectl", "config", "view"], stdout=subprocess.PIPE)
			grep = subprocess.Popen(["grep", "current-context"], stdout=subprocess.PIPE, stdin=kubectl_config.stdout,
			                        stderr=subprocess.STDOUT)

			kubectl_context_string = grep.communicate()
			kube_context = kubectl_context_string[0].split(' ')[1].strip()
			try:
				subprocess.check_call(
					["kubectl", "config", "set", "contexts.{context}.namespace".format(context=kube_context),
					 self.namespace_spec["metadata"]["name"]])
			except subprocess.CalledProcessError as e:
				filestore.logToMaster("Couldn't set cluster context: {reason}".format(reason=e))
				exit(-1)  # raise an exception

			filestore.logToMaster(
				"{timestamp}  Cluster configuration was successful!".format(timestamp=self.create_timestamp()))

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