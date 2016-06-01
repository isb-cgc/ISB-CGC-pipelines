import os
import sys
import math
import sqlite3
import requests
import pyinotify
import subprocess
from time import time, sleep
from datetime import datetime
from ConfigParser import SafeConfigParser


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
			self._pipelinesDb.execute(query)
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

