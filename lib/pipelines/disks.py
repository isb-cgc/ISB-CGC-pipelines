import os
import httplib2
from random import randint
from googleapiclient import discovery
from oauth2client.client import GoogleCredentials
from googleapiclient.errors import HttpError


class DataDiskError(Exception):
	def __init__(self, msg):
		super(DataDiskError, self).__init__()
		self.msg = msg


class DataDisk(object):
	def __init__(self, name, config, diskSize, diskType="PERSISTENT_SSD", zone=None, inputs=None):
		# submit a request to the gce api for a new disk with the given parameters
		# if inputs is not None, run a pipeline job to populate the disk
		self.projectId = config.project_id
		self.zones = [zone if zone is not None else x for x in config.zones.split(',')]

		credentials = GoogleCredentials.get_application_default()
		http = credentials.authorize(httplib2.Http())

		if credentials.access_token_expired:
			credentials.refresh(http)

		gce = discovery.build('compute', 'v1', http=http)

		diskTypes = {
			"PERSISTENT_HDD": "pd-standard",
			"PERSISTENT_SSD": "pd-ssd"
		}

		for z in self.zones:
			body = {
				"kind": "compute#disk",
				"zone": "projects/{projectId}/zones/{zone}".format(projectId=self.projectId, zone=z),
				"name": name,
				"sizeGb": diskSize,
				"type": "projects/{projectId}/zones/{zone}/diskTypes/{type}".format(projectId=self.projectId, zone=z, type=diskTypes[diskType])
			}

			try:
				resp = gce.disks().insert(project=self.projectId, zone=z, body=body).execute()
			except HttpError as e:
				raise DataDiskError("Couldn't create data disk {n}: {reason}".format(n=name, reason=e))

			while True:
				try:
					result = gce.zoneOperations().get(project=self.projectId, zone=z,  operation=resp['name']).execute()
				except HttpError:
					break
				else:
					if result['status'] == 'DONE':
						break


			# add the disk to the data_disks database

