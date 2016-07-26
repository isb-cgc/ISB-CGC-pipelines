import os
import pyinotify
from ConfigParser import SafeConfigParser, NoOptionError, NoSectionError
from pipelines.logger import PipelineLogger
from pipelines.paths import *


class PipelineConfigError(Exception):  # TODO: implement
	def __init__(self, msg):
		super(PipelineConfigError, self).__init__()
		self.msg = msg


class PipelineConfig(SafeConfigParser, object):
	def __init__(self, from_file=True, path=None, project_id=None, zones=None, scopes=None, service_account_email=None, max_running_jobs=None, autorestart_preempted=None):
		super(PipelineConfig, self).__init__()

		self._configParams = {
			"project_id": {
				"section": "gcp",
				"required": True,
				"default": None
			},
			"zones": {
				"section": "gcp",
				"required": True,
				"default": "us-central1-a,us-central1-b,us-central1-c,us-central1-f,us-east1-b,us-east1-c,us-east1-d"
			},
			"scopes": {
				"section": "gcp",
				"required": True,
				"default": "https://www.googleapis.com/auth/genomics,https://www.googleapis.com/auth/compute,https://www.googleapis.com/auth/devstorage.full_control"
			},
			"service_account_email": {
				"section": "gcp",
				"required": True,
				"default": "default"
			},
			"max_running_jobs": {
				"section": "pipelines",
				"required": True,
				"default": 2000
			},
			"autorestart_preempted": {
				"section": "pipelines",
				"required": True,
				"default": False
			}
		}

		if from_file:
			self._from_file = True

			if path is None:
				self.path = SERVER_CONFIG_PATH
			else:
				self.path = path

			try:
				os.makedirs(os.path.dirname(self.path))
			except OSError:
				pass

			self.refresh()

		else:
			self._from_file = False

			try:
				self.project_id = project_id

			except AttributeError as e:
				raise PipelineConfigError("Couldn't create config: {reason}".format(reason=e))

			else:
				for o in self._configParams.keys():
					s = self._configParams[o]["section"]

					if locals[o] is None:
						v = self._configParams[o]["default"]
					else:
						v = locals[o]

					self.update(s, o, v)

	def update(self, section, option, value, first_time=False):
		if option not in self._configParams.keys():
			raise PipelineConfigError("unrecognized option {s}/{o}".format(s=section, o=option))
		else:
			if self._configParams[option]["section"] != section:
				raise PipelineConfigError("unrecognized section {s}".format(s=section))

		if not self.has_section(section):
			self.add_section(section)

		self.set(section, str(option), str(value))

		if self._from_file:
			with open(self.path, 'w') as f:
				self.write(f)

		self.refresh()

	def watch(self):
		# watch changes to the config file -- needs to be run in a separate thread
		configStatusManager = pyinotify.WatchManager()
		configStatusNotifier = pyinotify.Notifier(configStatusManager)
		configStatusManager.add_watch(self.path, pyinotify.IN_CLOSE_WRITE, proc_fun=PipelineConfigUpdateHandler(config=self))
		configStatusNotifier.loop()

	def refresh(self):
		self.__dict__.update(self._verify())

	def _verify(self):
		try:
			if self._from_file:
				self.read(self.path)

		except IOError as e:
			raise PipelineConfigError("Couldn't open {path}: {reason}".format(path=self.path, reason=e))

		else:
			d = {}
			for name, attrs in self._configParams.iteritems():
				if attrs["required"]:
					if not self.has_section(attrs["section"]):
						raise PipelineConfigError("missing required section {s} in the configuration!\nRUN `isb-cgc-pipelines config` to correct the configuration".format(s=attrs["section"]))

					if not self.has_option(attrs["section"], name):
						raise PipelineConfigError("missing required option {o} in section {s}!\nRun `isb-cgc-pipelines config` to correct the configuration".format(s=attrs["section"], o=name))

				try:
					d[name] = self.get(attrs["section"], name)

				except NoOptionError:
					pass
				except NoSectionError:
					pass

			return d


class PipelineConfigUpdateHandler(pyinotify.ProcessEvent):
	def my_init(self, config=None):  # config -> PipelineConfig
		self._config = config

	def process_IN_CLOSE_WRITE(self, event):
		PipelineLogger.writeStdout("Refreshing configuration ...")
		self._config.refresh()
