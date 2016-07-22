import httplib2
from googleapiclient import discovery
from oauth2client.client import GoogleCredentials
from googleapiclient.errors import HttpError

from pipelines.service import PipelineService


class DataDiskError(Exception):
	def __init__(self, msg):
		super(DataDiskError, self).__init__()
		msg = msg


class DataDisk(object):
	@staticmethod
	def create(config, disk_name=None, disk_size=None, disk_type="PERSISTENT_SSD", zone=None):
		# submit a request to the gce api for a new disk with the given parameters
		# if inputs is not None, run a pipeline job to populate the disk
		projectId = config.project_id
		zones = [zone if zone is not None else x for x in config.zones.split(',')]

		credentials = GoogleCredentials.get_application_default()
		http = credentials.authorize(httplib2.Http())

		if credentials.access_token_expired:
			credentials.refresh(http)

		gce = discovery.build('compute', 'v1', http=http)

		diskTypes = {
			"PERSISTENT_HDD": "pd-standard",
			"PERSISTENT_SSD": "pd-ssd"
		}

		for z in zones:
			body = {
				"kind": "compute#disk",
				"zone": "projects/{projectId}/zones/{zone}".format(projectId=projectId, zone=z),
				"name": disk_name,
				"sizeGb": disk_size,
				"type": "projects/{projectId}/zones/{zone}/diskTypes/{type}".format(projectId=projectId, zone=z, type=diskTypes[disk_type])
			}

			try:
				resp = gce.disks().insert(project=projectId, zone=z, body=body).execute()
			except HttpError as e:
				raise DataDiskError("Couldn't create data disk {n}: {reason}".format(n=name, reason=e))

			while True:
				try:
					result = gce.zoneOperations().get(project=projectId, zone=z,  operation=resp['name']).execute()
				except HttpError:
					break
				else:
					if result['status'] == 'DONE':
						break

			# update the database ...
			PipelineService.sendRequest()

	@staticmethod
	def delete():  # TODO: implement
		pass

	@staticmethod
	def describe():  # TODO: implement
		pass


