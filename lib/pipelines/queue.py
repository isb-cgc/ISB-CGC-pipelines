import pika
from pipelines.scheduler import PipelineSchedulerUtils


class PipelineQueueUtils(object):
	def __init__(self, queueName):
		self._qname = queueName
		self._connection = pika.BlockingConnection(pika.ConnectionParameters(host='localhost'))
		self._channel = self._connection.channel()
		self._channel.queue_declare(queue=queueName, durable=True)

	def __del__(self):
		self._connection.close()

	def publish(self, msg):
		self._channel.basic_publish(exchange='', routing_key=self._qname, body=msg)

	def get(self):
		self._channel.basic_qos(prefetch_count=1)
		method, header, body = self._channel.basic_get(self._qname)

		return body, method

	def acknowledge(self, method):
		if method:
			self._channel.basic_ack(method.delivery_tag)
		else:
			PipelineSchedulerUtils.writeStdout("No message returned!")