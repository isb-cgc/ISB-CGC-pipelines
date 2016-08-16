import sys
from datetime import datetime
from time import time


class PipelineJobLogger(object):
	@staticmethod
	def createTimestamp():
		return datetime.fromtimestamp(time()).strftime('%Y-%m-%d %H:%M:%S')

	@staticmethod
	def writeStdout(s):
		ts = PipelineJobLogger.createTimestamp()
		sys.stdout.write('\t'.join([ts, s]) + '\n')
		sys.stdout.flush()

	@staticmethod
	def writeStderr(s):
		ts = PipelineJobLogger.createTimestamp()
		sys.stderr.write('\t'.join([ts, s]) + '\n')
		sys.stderr.flush()
