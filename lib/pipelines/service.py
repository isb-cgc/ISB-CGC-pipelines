from datetime import datetime
from googleapiclient.errors import HttpError


class PipelineServiceUtils:

	@staticmethod
	def bootstrapService(gke, config):
		pass

	@staticmethod
	def bootstrapMessageHandlers(pubsub, logging, config):  # TODO: move topics that aren't required to the configuration file
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