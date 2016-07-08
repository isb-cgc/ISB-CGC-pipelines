import pika


class PipelineQueueError(Exception):
	def __init__(self, msg):
		super(PipelineQueueError, self).__init__()
		self.msg = msg


class PipelineQueue(object):
	def __init__(self, queueName):
		self._qname = queueName
		self._connection = pika.BlockingConnection(pika.ConnectionParameters(host='localhost'))
		self._channel = self._connection.channel()
		self._channel.queue_declare(queue=queueName, durable=True)

	def __del__(self):
		try:
			self._connection.close()

		except pika.exceptions.AMPQError as e:
			pass

	def publish(self, msg):
		try:
			self._channel.basic_publish(exchange='', routing_key=self._qname, body=msg)

		except pika.exceptions.AMPQError as e:
			raise PipelineQueueError("Couldn't push message to queue: {reason}".format(reason=e))

	def get(self):
		self._channel.basic_qos(prefetch_count=1)
		try:
			method, header, body = self._channel.basic_get(self._qname)

		except pika.exceptions.AMPQError as e:
			raise PipelineQueueError("Couldn't get message from queue: {reason}".format(reason=e))

		return body, method

	def acknowledge(self, method):
		if method:
			try:
				self._channel.basic_ack(method.delivery_tag)

			except pika.exceptions.AMPQError as e:
				raise PipelineQueueError("Couldn't acknowledge message: {reason}".format(reason=e))
