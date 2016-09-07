import os
import sqlite3


class PipelineDatabaseError(Exception):
	def __init__(self, msg):
		super(PipelineDatabaseError, self).__init__()
		self.msg = msg


class DatabaseRecord(object):
	def __init__(self, innerDict):
		self.__dict__.update(innerDict)


class PipelineDatabase(object):
	def __init__(self, config):
		if config.db == "mysql":
			pass  # TODO: determine best production grade relational database to use

		elif config.db == "sqlite":
			self._dbConn = sqlite3.connect(os.environ["PIPELINES_DB"])

		self._pipelinesDb = self._dbConn.cursor()

	def __del__(self):
		self._dbConn.close()

	def closeConnection(self):
		self._dbConn.close()

	def _parseCriteria(self, c):
		if "operation" in c.keys() and "values" in c.keys():
			if c["operation"] in ["AND", "OR", "NOT"]:
				opString = " {op} ".format(op=c["operation"])
				assignments = []
				substitutions = []
				for value in c["values"]:
					s, v = zip(*self._parseCriteria(value))
					assignments.append(s)
					if v is not None:
						substitutions.append(v)

				return opString.join(assignments), substitutions

			else:
				raise PipelineDatabaseError("{op} not a valid operation!".format(op=c["operation"]))

		elif "key" in c.keys() and "value" in c.keys():
			if type(c["value"]) is dict and "incr" in c["value"].keys():
				return "{key} = {key} + {value}".format(key=c["key"], value=c["value"]["incr"]), None

			else:
				return "{key} = ?".format(key=c["key"]), c["value"]

		else:
			raise PipelineDatabaseError("Invalid parameters: {params}".format(params=','.join(c.keys())))

	def select(self, table, *data, **criteria):
		if len(data) > 0:
			dataString = ', '.join(data)
		else:
			dataString = "*"

		where, subs = self._parseCriteria(criteria)
		query = "SELECT {data} from {table} WHERE {where}".format(data=dataString, table=table, where=where)
		records = self._pipelinesDb.execute(query, tuple(subs)).fetchall()

		results = {
			"results": []
		}

		for r in records:
			o = {}
			for i, d in enumerate(data):
				o[d] = r[i]

			results["results"].append(o)

		return results

	def insert(self, table, **record):
		cols, vals = zip(*record.items())
		valSubs = ','.join(['?' for x in range(0, len(vals))])

		try:
			self._pipelinesDb.execute("INSERT INTO {table} ({cols}) VALUES ({valSubs})".format(table=table, cols=','.join(cols), valSubs=valSubs), tuple(vals))
			self._dbConn.commit()

		except sqlite3.Error as e:
			raise PipelineDatabaseError("Couldn't create record: {reason}".format(reason=e))

		return self._pipelinesDb.lastrowid

	def update(self, table, updates, criteria):
		query = "UPDATE {table} SET {values} WHERE {where}"
		updateCols, updateVals = zip(*updates.items())
		valString = ','.join(["{v} = ?".format(v=v) for v in updateCols])
		where, subs = self._parseCriteria(criteria)

		try:
			self._pipelinesDb.execute(query.format(table=table, values=valString, where=where), updateVals + tuple(subs))

		except sqlite3.Error as e:
			raise PipelineDatabaseError("Couldn't update table: {reason}".format(reason=e))

		else:
			self._dbConn.commit()

	def increment(self, table, column, incr, criteria):
		query = "UPDATE {table} SET {column} = {column} + {incr} WHERE {where}"
		where, subs = self._parseCriteria(criteria)

		try:
			self._pipelinesDb.execute(query.format(table=table, column=column, incr=str(incr), where=where), tuple(subs))

		except sqlite3.Error as e:
			raise PipelineDatabaseError("Couldn't increment value: {reason}".format(reason=e))

		else:
			self._dbConn.commit()

	def create(self, name, entity, criteria):
		check = 'SELECT name FROM sqlite_master WHERE type="{entity}" AND name="{name}"'.format(entity=entity, name=name)
		if len(self._pipelinesDb.execute(check).fetchall()) == 0:
			columns = ', '.join(["{col} {properties}".format(col=col, properties=properties) for col, properties in criteria.iteritems()])
			create = "CREATE {entity} {name} ({columns})".format(entity=entity, name=name, columns=columns)

			try:
				self._pipelinesDb.execute(create)

			except sqlite3.Error as e:
				raise PipelineDatabaseError("Couldn't create entity {entity} with name {name}: {reason}".format(entity=entity, name=name, reason=e))

			else:
				self._dbConn.commit()
