import os
import requests
import argparse

import containers
import container_scripts
from steps import *
from pipelines.utils import PipelineBuilder, PipelinesConfig
from pipelines.paths import CLIENT_CONFIG_PATH


parser = argparse.ArgumentParser()
parser.add_argument("--gdc-file-uuid", required=True)
parser.add_argument("--gdc-token-url", required=True)
parser.add_argument("--gdc-data-destination", required=True)
parser.add_argument("--gdc-download-cores", required=False, default=8)
parser.add_argument("--gdc-download-mem", required=False, default=8)
parser.add_argument("--fastqc-output-prefix", required=True)
parser.add_argument("--fastqc-data-destination", required=True)
parser.add_argument("--fastqc-cores", required=False, default=1),
parser.add_argument("--fastqc-mem", required=False, default=2)
parser.add_argument("--logs-destination", required=True)
parser.add_argument("--disk-size", required=True)
parser.add_argument("--disk-type", required=False, default="PERSISTENT_SSD")
parser.add_argument("--tag", required=True)
parser.add_argument("--preemptible", action="store_true", default=False)

args = parser.parse_args()

config = PipelinesConfig(CLIENT_CONFIG_PATH)
p = PipelineBuilder(config)

# get some information from the GDC about this particular file
metadata = requests.get("https://gdc-api.nci.nih.gov/files/{fileUuid}".format(fileUuid=args.gdc_file_uuid)).json()

gdcStep = GDCDownloadStep(args.gdc_file_uuid,
                          args.gdc_token_url,
                          args.gdc_data_destination,
                          args.disk_size,
                          args.disk_type,
                          args.logs_destination,
                          containers.GDC_DOWNLOAD_CONTAINER,
                          container_scripts.GDC_DOWNLOAD_SCRIPT,
                          args.tag,
                          args.gdc_download_cores,
                          args.gdc_download_mem,
                          args.preemptible).get()

fastqcStep = FastQCStep("{fastqcInput}".format(fastqcInput=os.path.join(args.gdc_data_destination, metadata["data"]["file_name"])),
                        args.fastqc_data_destination,
                        args.fastqc_output_prefix,
                        args.disk_size,
                        args.disk_type,
                        args.logs_destination,
                        containers.FASTQC_CONTAINER,
                        container_scripts.FASTQC_SCRIPT,
                        args.tag,
                        args.fastqc_cores,
                        args.fastqc_mem,
                        args.preemptible).get()

gdcStep.addChild(fastqcStep)

p.addStep(gdcStep)
p.addStep(fastqcStep)
p.run()
