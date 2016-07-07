import os
import math
import requests
import subprocess


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

		return int(math.ceil(scalingFactor * fileSize/ roundTo) * roundTo) / 1000000000

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

			objectPath = "{diseaseCode}/{analyteType}/{libraryStrategy}/{centerName}/{platform}/".format(
				diseaseCode=diseaseCode, analyteType=analyteType, libraryStrategy=libraryStrategy,
				centerName=centerName, platform=platform)

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

