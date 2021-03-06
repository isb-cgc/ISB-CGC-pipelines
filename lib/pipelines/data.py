import os
import math
import json
import httplib2
import requests
import subprocess
import pprint

from googleapiclient import discovery
from oauth2client.client import GoogleCredentials
from googleapiclient.errors import HttpError


class DataUtilsError(Exception):
	def __init__(self, msg):
		super(DataUtilsError, self).__init__()
		self.msg = msg

class GoogleApiService(object):
	@staticmethod
	def create(apiName, apiVersion):
		credentials = GoogleCredentials.get_application_default()
		http = credentials.authorize(httplib2.Http())

		if credentials.access_token_expired:
			credentials.refresh(http)

		return discovery.build(apiName, apiVersion, http)

class GDCDataUtils(object):
	@ staticmethod
	def query(tokenFile, endpoint, query="", params=None):
		url = "https://gdc-api.nci.nih.gov/{e}{q}".format(e=endpoint, q=query)

		with open(tokenFile) as f:
			token = f.read()

		headers = {
			"Accept": "application/json",
			"X-Auth-Token": token
		}

		return requests.get(url, params=params, headers=headers)

	@staticmethod
	def getFilenames(fileUuid, tokenFile):
		filters = {
			"op": "=",
			"content": {
				"field": "file_id",
				"value": [fileUuid]
			}
		}

		params = {
			"filters": json.dumps(filters)
		}

		fileInfo = GDCDataUtils.query(tokenFile, "files", params=params)

		return [str(x["file_name"]) for x in fileInfo.json()["data"]["hits"][0]]

	@staticmethod
	def getFilesize(fileUuid, tokenFile=None, projectId=None):
		if tokenFile:
			filters = {
				"op": "=",
				"content": {
					"field": "file_id",
					"value": [fileUuid]
				}
			}

			params = {
				"filters": json.dumps(filters)
			}

			fileInfo = GDCDataUtils.query(tokenFile, "files", params=params)

			return int(fileInfo.json()["data"]["hits"][0]["file_size"])

		else:
			bq = GoogleApiService.create('bq', 'v2')
			body = {
				"query": "SELECT SUM(a_file_size) FROM GDC_metadata.GCS_join1 WHERE file_id = {fileUuid}".format(fileUuid=fileUuid)
			}
			results = bq.jobs().query(projectId=projectId, body=body).execute()
			return results["rows"][results["rows"].keys()[0]][0]

	@staticmethod
	def constructGCSFilePath(fileUuid, tokenFile):
		filters = {
			"op": "=",
			"content": {
				"field": "file_id",
				"value": [fileUuid]
			}
		}

		params = {
			"filters": json.dumps(filters)
		}

		query = "?expand=cases.project"

		fileInfo = GDCDataUtils.query(tokenFile, "files", query=query, params=params).json()
		pprint.pprint(fileInfo)

		return "{project}/{strategy}/{platform}/{uuid}/{filename}".format(
			project=fileInfo["data"]["hits"][0]["cases"][0]["project"]["project_id"],
			strategy=str(fileInfo["data"]["hits"][0]["experimental_strategy"]),
			platform=str(fileInfo["data"]["hits"][0]["platform"]),
			uuid=str(fileUuid),
			filename=str(fileInfo["data"]["hits"][0]["file_name"])
		)

	@staticmethod
	def calculateDiskSize(tokenFile=None, fileUuid=None, inputFiles=None, inputFileSize=None, scalingFactor=None, roundToNearestGbInterval=None):
		if inputFiles is not None:
			fileSize = 0
			for f in inputFiles:
				fileSize += int(subprocess.check_output(["gsutil", "du", f]).split(' ')[0])

		elif fileUuid is not None:
			fileSize = GDCDataUtils.getFilesize(fileUuid, tokenFile)

		elif inputFileSize is not None:
			filesize = inputFileSize

		else:
			raise DataUtilsError("Couldn't determine disk size! Please provide a path to an existing file in GCS or a file uuid from the GDC.")

		if scalingFactor is not None:
			scalingFactor = int(scalingFactor)
		else:
			scalingFactor = 1

		if roundToNearestGbInterval is not None:
			roundTo = float(roundToNearestGbInterval) * 1000000000

		else:
			roundTo = 1

		return int(math.ceil(scalingFactor * fileSize / roundTo) * roundTo) / 1000000000


class CGHubDataUtils(object):
	@staticmethod
	def getAnalysisDetail(analysisId):
		cghubMetadataUrl = "https://cghub.ucsc.edu/cghub/metadata/analysisDetail?analysis_id={analysisId}"

		headers = {
			"Accept": "application/json"
		}

		return requests.get(cghubMetadataUrl.format(analysisId=analysisId), headers=headers).json()

	@staticmethod
	def getFilenames(analysisId):
		analysisDetail = CGHubDataUtils.getAnalysisDetail(analysisId)
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
			analysisDetail = CGHubDataUtils.getAnalysisDetail(analysisId)

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

		return int(math.ceil(scalingFactor * fileSize/ roundTo) * roundTo) / 1000000000

	@staticmethod
	def constructObjectPath(analysisId, outputPath):

		analysisDetail = CGHubDataUtils.getAnalysisDetail(analysisId)

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

			objectPath = "{diseaseCode}/{analyteType}/{libraryStrategy}/{centerName}/{platform}/".format(
				diseaseCode=diseaseCode, analyteType=analyteType, libraryStrategy=libraryStrategy,
				centerName=centerName, platform=platform)

			return os.path.join(outputPath, objectPath)

		else:
			raise LookupError("ERROR: no files found for analysis ID {a}!".format(a=analysisId))

	@staticmethod
	def getChecksum(analysisId):
		analysisDetail = CGHubDataUtils.getAnalysisDetail(analysisId)
		if len(analysisDetail["result_set"]["results"]) > 0:
			for f in analysisDetail["result_set"]["results"][0]["files"]:
				return f["checksum"]["#text"]
		else:
			raise LookupError("ERROR: no files found for analysis ID {a}!".format(a=analysisId))

