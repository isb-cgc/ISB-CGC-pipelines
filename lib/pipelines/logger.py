import sys
from datetime import datetime
from time import time


class PipelineLogger(object):
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
