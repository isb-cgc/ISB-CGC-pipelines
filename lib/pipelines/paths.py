import os

MODULE_PATH = "/usr/local/ISB-CGC-pipelines/lib"
TEMPLATES_PATH = "/usr/local/ISB-CGC-pipelines/lib/k8s/templates"
SERVER_CONFIG_PATH = "/etc/isb-cgc-pipelines/config"
CLIENT_CONFIG_PATH = os.path.join(os.environ["HOME"], ".isb-cgc-pipelines", "config")
SERVER_LOG_PATH = "/isb-cgc-pipelines/logs"