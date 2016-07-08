import os
import json
import subprocess
from tempfile import mkstemp
from pipelines.db import PipelineDatabase, PipelineDatabaseError
from pipelines.schema import PipelineSchema
from pipelines.builder import PipelineBuilder, PipelineSchemaValidationError, PipelineSubmitError
from pipelines.queue import PipelineQueue, PipelineQueueError

# TODO: break this class into multiple classes and move anything using the db utils class to another file


class PipelineSchedulerError(Exception):
	def __init__(self, msg):
		super(PipelineSchedulerError, self).__init__()
		self.msg = msg

class PipelineScheduler(object):
	@staticmethod
	def submitPipeline(config, pipelineName=None, logsPath=None, imageName=None, scriptUrl=None, cmd=None, cores=None, mem=None, diskSize=None, diskType=None, env=None, inputs=None, outputs=None, tag=None, preemptible=None, syncOutputs=None):

		# send the request message to the local rabbitmq server
		if scriptUrl:
			pipelineSpec = PipelineSchema(pipelineName, config, logsPath, imageName,
			                              scriptUrl=scriptUrl, cores=cores,
			                              mem=mem, diskSize=diskSize, diskType=diskType, env=env,
			                              inputs=inputs, outputs=outputs, tag=tag,
			                              preemptible=preemptible, syncOutputs=syncOutputs)
		elif cmd:
			pipelineSpec = PipelineSchema(pipelineName, config, logsPath, imageName,
			                              cmd=cmd, cores=cores,
			                              mem=mem, diskSize=diskSize, diskType=diskType, env=env,
			                              inputs=inputs, outputs=outputs, tag=tag,
			                              preemptible=preemptible, syncOutputs=syncOutputs)

		pipelineBuilder = PipelineBuilder(config)
		pipelineBuilder.addStep(pipelineSpec)

		try:
			pipelineBuilder.run()
		except PipelineSchemaValidationError as e:
			raise PipelineSchedulerError("Schema validation failed: {reason}".format(reason=e))
		except PipelineSubmitError as e:
			raise PipelineSchedulerError("Submission failed: {reason}".format(reason=e))


	@staticmethod
	def stopPipeline(args, config):
		pipelineQueue = PipelineQueue('CANCEL_Q')

		pipelineDatabase = PipelineDatabase(config)

		if args.jobId:
			try:
				jobInfo = pipelineDatabase.getJobInfo(select=["current_status", "operation_id", "job_id"], where={"job_id": args.jobId})

			except PipelineDatabaseError as e:
				raise PipelineSchedulerError("Couldn't stop pipeline: {reason}".format(reason=e))

		elif args.pipeline:
			try:
				jobInfo = pipelineDatabase.getJobInfo(select=["current_status", "operation_id", "job_id"], where={"pipeline_name": args.pipeline})

			except PipelineDatabaseError as e:
				raise PipelineSchedulerError("Couldn't stop pipeline: {reason}".format(reason=e))

		elif args.tag:
			try:
				jobInfo = pipelineDatabase.getJobInfo(select=["current_status", "operation_id", "job_id"], where={"tag": args.tag})

			except PipelineDatabaseError as e:
				raise PipelineSchedulerError("Couldn't stop pipeline: {reason}".format(reason=e))

		for j in jobInfo:
			if j.current_status == "RUNNING":
				msg = {
					"job_id": j.job_id,
					"operation_id": j.operation_id
				}
				try:
					pipelineQueue.publish(json.dumps(msg))
				except PipelineQueueError as e:
					raise PipelineSchedulerError("Couldn't stop pipeline: {reason}".format(reason=e))


	@staticmethod
	def restartJobs(args, config):  # TODO: reimplement
		pipelineDatabase = PipelineDatabase(config)
		pipelineQueue = PipelineQueue('WAIT_Q')

		if args.jobId:
			try:
				jobInfo = pipelineDatabase.getJobInfo(select=["request"], where={"job_id": args.jobId})[0].request

			except PipelineDatabaseError as e:
				raise PipelineSchedulerError("Couldn't restart pipeline: {reason}".format(reason=e))

			request = json.loads(jobInfo)
			msg = {
				"job_id": args.jobId,
				"request": request
			}

			try:
				pipelineQueue.publish(json.dumps(msg))

			except PipelineQueueError as e:
				raise PipelineSchedulerError("Couldn't restart pipeline: {reason}".format(reason=e))

		if args.preempted:
			try:
				preempted = pipelineDatabase.getJobInfo(select=["job_id", "request"], where={"current_status": "PREEMPTED"})

			except PipelineDatabaseError as e:
				raise PipelineSchedulerError("Couldn't restart pipeline: {reason}".format(reason=e))

			for p in preempted:
				msg = {
					"job_id": p.job_id,
					"request": json.loads(p.request)
				}

				try:
					pipelineQueue.publish(json.dumps(msg))

				except PipelineQueueError as e:
					raise PipelineSchedulerError("Couldn't restart pipeline: {reason}".format(reason=e))

	@staticmethod
	def getJobLogs(args, config):  # TODO: reimplement
		pipelineDatabase = PipelineDatabase(config)

		try:
			jobInfo = pipelineDatabase.getJobInfo(select=["stdout_log", "stderr_log", "gcs_log_path"], where={"job_id": args.jobId})

		except PipelineDatabaseError as e:
			raise PipelineSchedulerError("Couldn't get job logs: {reason}".format(reason=e))

		with open(os.devnull, 'w') as fnull:
			if args.stdout:
				try:
					stdoutLogFile = subprocess.check_output(["gsutil", "cat", os.path.join(jobInfo[0].gcs_log_path, jobInfo[0].stdout_log)], stderr=fnull)
				except subprocess.CalledProcessError as e:
					raise PipelineSchedulerError("Couldn't get the stdout log : {reason}".format(reason=e))

				print "STDOUT:\n"
				print stdoutLogFile
				print "---------\n"

			if args.stderr:
				try:
					stderrLogFile = subprocess.check_output(["gsutil", "-q", "cat", os.path.join(jobInfo[0].gcs_log_path, jobInfo[0].stderr_log)], stderr=fnull)

				except subprocess.CalledProcessError as e:
					raise PipelineSchedulerError("Couldn't get the stderr log : {reason}".format(reason=e))

				print "STDERR:\n"
				print stderrLogFile
				print "---------\n"

	@staticmethod
	def getJobsList(config, pipeline=None, tag=None, status=None, createTimeAfter=None, limit=None):  # TODO: reimplement
		header = "JOBID%sPIPELINE%sOPERATION ID%sTAG%sSTATUS%sCREATE TIME%sPREEMPTIONS\n"
		jobStatusString = "{jobId}%s{pipeline}%s{operationId}%s{tag}%s{status}%s{createTime}%s{preemptions}\n"

		pipelineDatabase = PipelineDatabase(config)

		select = ["job_id", "operation_id", "pipeline_name", "tag", "current_status", "create_time", "preemptions"]
		where = {}

		if pipeline:
			where["pipeline_name"] = pipeline

		if tag:
			where["tag"] = tag

		if status:
			where["current_status"] = status

		if createTimeAfter:
			where["create_time"] = {
				"value": createTimeAfter,
				"operator": ">="
			}

		try:
			jobs = pipelineDatabase.getJobInfo(select=select, where=where)

		except PipelineDatabaseError as e:
			raise PipelineSchedulerError("Couldn't get the jobs list: {reason}".format(reason=e))

		else:
			def fieldPadding(maxLen, actualLen):
				return ''.join([' ' for x in range(maxLen - actualLen + 4)])

			if len(jobs) > 0:
				jobStrings = []

				jobIdLengths = [len(str(x.job_id)) if x.job_id is not None else len(str("None")) for x in jobs]
				jobIdLengths.append(len("JOBID"))
				maxJobIdLen = max(jobIdLengths)

				pipelineLengths = [len(x.pipeline_name) if x.pipeline_name is not None else len(str("None")) for x in
				                   jobs]
				pipelineLengths.append(len("PIPELINE"))
				maxPipelineLen = max(pipelineLengths)

				operationIdLengths = [len(x.operation_id) if x.operation_id is not None else len(str("None")) for x in
				                      jobs]
				operationIdLengths.append(len("OPERATION ID"))
				maxOperationIdLen = max(operationIdLengths)

				statusLengths = [len(x.current_status) if x.current_status is not None else len(str("None")) for x in
				                 jobs]
				statusLengths.append(len("STATUS"))
				maxStatusLen = max(statusLengths)

				tagLengths = [len(x.tag) if x.tag is not None else len(str("None")) for x in jobs]
				tagLengths.append(len("TAG"))
				maxTagLen = max(tagLengths)

				createTimeLengths = [len(x.create_time) if x.create_time is not None else len(str("None")) for x in
				                     jobs]
				createTimeLengths.append(len("CREATE TIME"))
				maxCreateTimeLen = max(createTimeLengths)

				jobStrings.append \
					(header % (fieldPadding(maxJobIdLen, len("JOBID")), fieldPadding(maxPipelineLen, len("PIPELINE")),
				                            fieldPadding(maxOperationIdLen, len("OPERATION ID")),
				                            fieldPadding(maxTagLen, len("TAG")),
				                            fieldPadding(maxStatusLen, len("STATUS")),
				                            fieldPadding(maxCreateTimeLen, len("CREATE TIME"))))

				for j in jobs:
					if j.create_time is not None:
						ct = j.create_time
					else:
						ct = "None"
					if j.operation_id is not None:
						op = j.operation_id
					else:
						op = "None"

					jobStrings.append \
						(jobStatusString.format(jobId=j.job_id, pipeline=j.pipeline_name, operationId=op, tag=j.tag,
					                                         status=j.current_status, createTime=ct, preemptions=j.preemptions) % (
						                  fieldPadding(maxJobIdLen, len(str(j.job_id))),
						                  fieldPadding(maxPipelineLen, len(j.pipeline_name)),
						                  fieldPadding(maxOperationIdLen, len(op)), fieldPadding(maxTagLen, len(j.tag)),
						                  fieldPadding(maxStatusLen, len(j.current_status)),
						                  fieldPadding(maxCreateTimeLen, len(ct))))

			return jobStrings


	@staticmethod  # TODO: split this function so that manual edits happen client side, while programmatic edits happen server side
	def editPipeline(args, config):
		pipelineDatabase = PipelineDatabase(config)

		try:
			request = json.loads(pipelineDatabase.getJobInfo(select=["request"], where={"job_id": args.jobId})[0].request)
		except PipelineDatabaseError as e:
			raise PipelineSchedulerError("Couldn't edit the pipeline: {reason}".format(reason=e))

		_, tmp = mkstemp()
		with open(tmp, 'w') as f:
			f.write("{data}".format(data=json.dumps(request, indent=4)))

		if "EDITOR" in os.environ.keys():
			editor = os.environ["EDITOR"]
		else:
			editor = "/usr/bin/nano"

		if subprocess.call([editor, tmp]) == 0:
			with open(tmp, 'r') as f:
				request = json.load(f)

			pipelineDatabase.updateJob(args.jobId, keyName="job_id", setValues={"request": json.dumps(request)})
		else:
			print "ERROR: there was a problem editing the request"
			exit(-1)