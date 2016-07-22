import os
import pyinotify
from ConfigParser import SafeConfigParser, NoOptionError, NoSectionError
from pipelines.scheduler import PipelineScheduler
from pipelines.paths import *


class PipelineConfigError(Exception):  # TODO: implement
	def __init__(self, msg):
		super(PipelineConfigError, self).__init__()
		self.msg = msg


class PipelineConfig(SafeConfigParser, object):
	def __init__(self, path=SERVER_CONFIG_PATH, first_time=False):
		super(PipelineConfig, self).__init__()

		self.path = path

		try:
			os.makedirs(os.path.dirname(self.path))
		except OSError:
			pass

		self._configParams = {
			"project_id": {
				"section": "gcp",
				"required": True,
				"default": None,
				"message": "Enter your GCP project ID: "
			},
			"zones": {
				"section": "gcp",
				"required": True,
				"default": "us-central1-a,us-central1-b,us-central1-c,us-central1-f,us-east1-b,us-east1-c,us-east1-d",
				"message": "Enter a comma-delimited list of GCE zones (leave blank to use the default list of all US zones): "
			},
			"scopes": {
				"section": "gcp",
				"required": True,
				"default": "https://www.googleapis.com/auth/genomics,https://www.googleapis.com/auth/compute,https://www.googleapis.com/auth/devstorage.full_control",
				"message": "Enter a comma-delimited list of GCP scopes (leave blank to use the default list of scopes): "
			},
			"service_account_email": {
				"section": "gcp",
				"required": True,
				"default": "default",
				"message": "Enter a valid service account email (leave blank to use the default service account): "
			},
			"max_running_jobs": {
				"section": "pipelines",
				"required": True,
				"default": 2000,
				"message": "Enter the maximum number of running jobs for any given time (leave blank to use default 2000): "
			},
			"autorestart_preempted": {
				"section": "pipelines",
				"required": True,
				"default": False,
				"message": "Would you like to automatically restart preempted jobs? (Only relevant when submitting jobs with the '--preemptible' flag; default is No) Y/N : "
			}
		}

		if first_time:
			for option, attrs in self._configParams.iteritems():
				if not self.has_section(attrs["section"]):
					self.add_section(attrs["section"])

				if attrs["required"]:
					val = raw_input(attrs["message"])
					if len(val) == 0:
						self.update(attrs["section"], option, attrs["default"], first_time=True)
					else:
						self.update(attrs["section"], option, val, first_time=True)

		self.refresh()

	def update(self, section, option, value, first_time=False):
		if option not in self._configParams.keys():
			raise PipelineConfigError("unrecognized option {s}/{o}".format(s=section, o=option))
		else:
			if self._configParams[option]["section"] != section:
				raise PipelineConfigError("unrecognized section {s}".format(s=section))

		if not self.has_section(section):
			self.add_section(section)

		self.set(section, str(option), str(value))

		with open(self.path, 'w') as f:
			self.write(f)

		if not first_time:
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
