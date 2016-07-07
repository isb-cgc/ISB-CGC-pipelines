import os
import sys
import pika
import json
import sqlite3
import subprocess
from time import time
from tempfile import mkstemp
from datetime import datetime
from ConfigParser import SafeConfigParser, NoOptionError, NoSectionError
from pipelines.db import PipelineDbUtils

# TODO: break this class into multiple classes and move anything using the db utils class to another file

class PipelineSchedulerUtils(object):
	@staticmethod
	def startScheduler(config, user):
		# set up the rabbitmq queues
		rabbitmqConn = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
		channel = rabbitmqConn.channel()

		channel.queue_declare(queue='WAIT_Q', durable=True)
		channel.queue_declare(queue='CANCEL_Q', durable=True)
		rabbitmqConn.close()

		pipelineDbUtils = PipelineDbUtils(config)
		pipelineDbUtils.createJobTables()

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
			c.set("program:pipelineJobScheduler", "command",
			      "pipelineJobScheduler --config {config}".format(config=config.path))
			c.set("program:pipelineJobScheduler", "environment",
			      "PYTHONPATH={modulePath}".format(modulePath=MODULE_PATH))
			c.set("program:pipelineJobScheduler", "numprocs", "1")
			c.set("program:pipelineJobScheduler", "autostart", "true")
			c.set("program:pipelineJobScheduler", "autorestart", "true")
			c.set("program:pipelineJobScheduler", "user", user)

			c.set("program:pipelineJobCanceller", "process_name", "pipelineJobCanceller")
			c.set("program:pipelineJobCanceller", "command",
			      "pipelineJobCanceller --config {config}".format(config=config.path))
			c.set("program:pipelineJobCanceller", "environment",
			      "PYTHONPATH={modulePath}".format(modulePath=MODULE_PATH))
			c.set("program:pipelineJobCanceller", "numprocs", "1")
			c.set("program:pipelineJobCanceller", "autostart", "true")
			c.set("program:pipelineJobCanceller", "autorestart", "true")
			c.set("program:pipelineJobCanceller", "user", user)

			c.set("program:pipelineInsertLogsHandler", "process_name", "%(program_name)s_%(process_num)s")
			c.set("program:pipelineInsertLogsHandler", "command",
			      "receivePipelineVmLogs --config {config} --subscription pipelineVmInsert".format(config=config.path))
			c.set("program:pipelineInsertLogsHandler", "environment",
			      "PYTHONPATH={modulePath}".format(modulePath=MODULE_PATH))
			c.set("program:pipelineInsertLogsHandler", "numprocs",
			      "10")  # TODO: come up with a formula for determining the number of processes
			c.set("program:pipelineInsertLogsHandler", "autostart", "true")
			c.set("program:pipelineInsertLogsHandler", "autorestart", "true")
			c.set("program:pipelineInsertLogsHandler", "user", user)

			c.set("program:pipelinePreemptedLogsHandler", "process_name", "%(program_name)s_%(process_num)s")
			c.set("program:pipelinePreemptedLogsHandler", "command",
			      "receivePipelineVmLogs --config {config} --subscription pipelineVmPreempted".format
				      (config=config.path))
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

		pipelineBuilder.run()

	@staticmethod
	def stopPipeline(args, config):
		pipelineQueueUtils = PipelineQueueUtils('CANCEL_Q')

		pipelineDbUtils = PipelineDbUtils(config)

		if args.jobId:
			jobInfo = pipelineDbUtils.getJobInfo(select=["current_status", "operation_id", "job_id"],
			                                     where={"job_id": args.jobId})

		elif args.pipeline:
			jobInfo = pipelineDbUtils.getJobInfo(select=["current_status", "operation_id", "job_id"],
			                                     where={"pipeline_name": args.pipeline})

		elif args.tag:
			jobInfo = pipelineDbUtils.getJobInfo(select=["current_status", "operation_id", "job_id"],
			                                     where={"tag": args.tag})

		for j in jobInfo:
			if j.current_status == "RUNNING":
				msg = {
					"job_id": j.job_id,
					"operation_id": j.operation_id
				}
				pipelineQueueUtils.publish(json.dumps(msg))


	@staticmethod
	def restartJobs(args, config):  # TODO: reimplement
		pipelineDbUtils = PipelineDbUtils(config)
		pipelineQueueUtils = PipelineQueueUtils('WAIT_Q')

		if args.jobId:
			request = json.loads \
				(pipelineDbUtils.getJobInfo(select=["request"], where={"job_id": args.jobId})[0].request)
			msg = {
				"job_id": args.jobId,
				"request": request
			}
			pipelineQueueUtils.publish(json.dumps(msg))

		if args.preempted:
			preempted = pipelineDbUtils.getJobInfo(select=["job_id", "request"], where={"current_status": "PREEMPTED"})

			for p in preempted:
				msg = {
					"job_id": p.job_id,
					"request": json.loads(p.request)
				}
				pipelineQueueUtils.publish(json.dumps(msg))

	@staticmethod
	def getJobLogs(args, config):  # TODO: reimplement
		pipelineDbUtils = PipelineDbUtils(config)

		jobInfo = pipelineDbUtils.getJobInfo(select=["stdout_log", "stderr_log", "gcs_log_path"],
		                                     where={"job_id": args.jobId})

		with open(os.devnull, 'w') as fnull:
			if args.stdout:
				try:
					stdoutLogFile = subprocess.check_output(
						["gsutil", "cat", os.path.join(jobInfo[0].gcs_log_path, jobInfo[0].stdout_log)], stderr=fnull)
				except subprocess.CalledProcessError as e:
					print "ERROR: couldn't get the stdout log : {reason}".format(reason=e)
					exit(-1)

				print "STDOUT:\n"
				print stdoutLogFile
				print "---------\n"

			if args.stderr:
				try:
					stderrLogFile = subprocess.check_output(
						["gsutil", "-q", "cat", os.path.join(jobInfo[0].gcs_log_path, jobInfo[0].stderr_log)],
						stderr=fnull)
				except subprocess.CalledProcessError as e:
					print "ERROR: couldn't get the stderr log : {reason}".format(reason=e)
					exit(-1)

				print "STDERR:\n"
				print stderrLogFile
				print "---------\n"

		pipelineDbUtils.closeConnection()

	@staticmethod
	def getJobsList(config, pipeline=None, tag=None, status=None, createTimeAfter=None, limit=None):  # TODO: reimplement
		header = "JOBID%sPIPELINE%sOPERATION ID%sTAG%sSTATUS%sCREATE TIME%sPREEMPTIONS\n"
		jobStatusString = "{jobId}%s{pipeline}%s{operationId}%s{tag}%s{status}%s{createTime}%s{preemptions}\n"

		pipelineDbUtils = PipelineDbUtils(config)

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
			jobs = pipelineDbUtils.getJobInfo(select=select, where=where)

		except sqlite3.OperationalError as e:
			raise ValueError("Couldn't get the jobs list: {reason}".format(reason=e))

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


	@staticmethod
	def editPipeline(args, config):
		pipelineDbUtils = PipelineDbUtils(config)

		request = json.loads(pipelineDbUtils.getJobInfo(select=["request"], where={"job_id": args.jobId})[0].request)

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

			pipelineDbUtils.updateJob(args.jobId, keyName="job_id", setValues={"request": json.dumps(request)})
		else:
			print "ERROR: there was a problem editing the request"
			exit(-1)

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