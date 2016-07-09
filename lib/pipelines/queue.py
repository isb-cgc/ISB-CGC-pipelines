import pika
import pika.exceptions


class PipelineQueueError(Exception):
	def __init__(self, msg):
		super(PipelineQueueError, self).__init__()
		self.msg = msg


class PipelineChannel(object):
	def __init__(self, host='localhost', port=5672):
		self._connection = pika.BlockingConnection(pika.ConnectionParameters(host=host, port=port))
		self._channel = self._connection.channel()

	def __del__(self):
		try:
			self._connection.close()

		except pika.exceptions.AMQPError as e:
			pass

	def get(self):
		pass  # TODO: implement

	def publishToExchange(self, exchangeName, routingKey, msg):
		try:
			self._channel.basic_publish(exchange=exchangeName, routing_key=routingKey, body=msg)

		except pika.exceptions.AMQPError as e:
			raise PipelineQueueError("Couldn't push message to exchange: {reason}".format(reason=e))


class PipelineExchange(object):
	def __init__(self, exchangeName, host='localhost', port=5672):
		self._exchangeName = exchangeName
		self._connection = pika.BlockingConnection(pika.ConnectionParameters(host=host, port=port))
		self._channel = self._connection.channel()
		self._channel.exchange_declare(exchange=exchangeName, type='direct')

	def __del__(self):
		try:
			self._connection.close()

		except pika.exceptions.AMQPError as e:
			pass


class PipelineQueue(object):
	def __init__(self, queueName, host='localhost', port=5672):
		self._qname = queueName
		self._connection = pika.BlockingConnection(pika.ConnectionParameters(host=host, port=port))
		self._channel = self._connection.channel()
		self._channel.queue_declare(queue=queueName, durable=True)
		self._exchange = ''

	def __del__(self):
		try:
			self._connection.close()

		except pika.exceptions.AMQPError as e:
			pass

	def publish(self, msg):
		try:
			self._channel.basic_publish(exchange=self._exchange, routing_key=self._qname, body=msg)

		except pika.exceptions.AMQPError as e:
			raise PipelineQueueError("Couldn't push message to queue: {reason}".format(reason=e))

	def get(self):
		self._channel.basic_qos(prefetch_count=1)
		try:
			method, header, body = self._channel.basic_get(self._qname)

		except pika.exceptions.AMQPError as e:
			raise PipelineQueueError("Couldn't get message from queue: {reason}".format(reason=e))

		return body, method

	def acknowledge(self, method):
		if method:
			try:
				self._channel.basic_ack(method.delivery_tag)

			except pika.exceptions.AMQPError as e:
				raise PipelineQueueError("Couldn't acknowledge message: {reason}".format(reason=e))

	def bindToExchange(self, exchangeName, routingKey):
		self._channel.queue_bind(exchange=exchangeName, queue=self._qname, routing_key=routingKey)
		self._exchange = exchangeName

	def unbindFromExchange(self):
		pass  # TODO: implement (may not be necessary
