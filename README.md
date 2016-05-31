# ISB-CGC-pipelines

A framework for running bioinformatic workflows and pipelines at scale using the Google Pipelines API as the underlying task-runner.

## Prerequisites

In order to use the ISB-CGC-pipelines tools, the following requirements must be met:
- You must be an owner or editor of a Google Cloud Project
- You must have the following APIs enabled in your Google Cloud Project: Google Compute Engine, Google Genomics
- (Optional) Install the Google Cloud SDK, including "alpha" subcommands for the `gcloud`.  

## Installation

### Method 1: Manual Installation

To install the tools manually, run the following commands:

```
# clone the repo

git clone https://github.com/isb-cgc/ISB-CGC-pipelines.git

# install

cd ISB-CGC-pipelines && sudo ./install.sh
```

### Method 2: Use a startup script on Google Compute Engine

A special startup script has been developed by the ISB for installing some common tools in an automated fashion on a Google Compute Engine VM.  ISB-CGC-pipelines is installed by default when the startup script is used to bootstrap a VM.  

To use the startup script, make sure that you're current workstation is equipped with the Google Cloud SDK and then run the following command (or a variation thereof) to start the bootstrapping process:

```
gcloud compute instances create my-pipeline-workstation --metadata startup-script-url=gs://isb-cgc-open/vm-startup-scripts/isb-cgc-workstation-startup.sh
```

Once the instance is ready, you can ssh to it using the following command:

```
gcloud compute ssh my-pipeline-workstation
```

For more information about the `gcloud compute` command, please refer to the [documentation](https://cloud.google.com/compute/docs/gcloud-compute/).

## Basic Usage

The most basic way to use the tool is to use the built-in command line utility, `isb-cgc-pipelines`.  The following sections will describe what the various subcommands do, but you can run `isb-cgc-pipelines -h` at any time to print usage information.

### Configuration

To configure the tool, run the following command and follow the prompts:

`isb-cgc-pipelines config set all`

### Starting and Stopping the Scheduler

To start the scheduler:

`isb-cgc-pipelines scheduler start`

To stop the scheduler:

`isb-cgc-pipelines scheduler stop`

### Submitting a Task

To submit a task, you can use the `submit` subcommand, which takes the following possible arguments:

| Argument | Required? | Description | Default |
| -------- | --------- | ----------- | ------------- |
| --pipelineName | Yes | A name for this particular pipeline (need not be unique) | None |
| --tag | No | An arbitrary identifier | Will be automatically generated using Python's uuid module (uuid4) |
| --logsPath | Yes | Relative path to a directory or bucket in Google Cloud Storage where log files will automatically be generated | None |
| --diskSize | Yes | The size of the data disk, in GB | None |
| --diskType | No | The type of disk to use; choose either "PERSISTENT_HDD" or "PERSISTENT_SSD" | "PERSISTENT_SSD" |
| --cores | No | The number of cores to request for the task | 1 |
| --mem | No | The amount of memory to request for the task, in GB | 1 |
| --scriptUrl or --cmd | Yes (choose one) | If `scriptUrl` is provided, the given Google Cloud Storage URL should point to a shell script; if `cmd` is provided, the argument should be a command string | None |
| --imageName | Yes | The fully-qualified Docker image name; used for specifying the runtime environment for the command or script you plan to run | None |
| --inputs | No | The input map, represented as a comma separated list of key:value pairs, where keys are Google Cloud Storage URLs and values are paths relative to the "working" directory of the task; e.g., gs://my-bucket/input1.txt:relative/path/input1.txt,gs://my-bucket/input2.txt:relative/path/input2.txt,... | None |
| --outputs | No | The output map, represented as a comma separated list of key:value pairs, where keys are the relative paths to output files (wrt the "working" directory) and values are destination GCS URLs where those files should be copied to when the task is finished; e.g., relative/path/output1.txt:gs://my-bucket/my-outputs/,relative/path/output2.txt:gs://my-bucket/my-outputs/ | None |
| --env | No | The environment map, represented as a comma separated list of key=value pairs, where keys are names of environment variables that you wish to set for the script or command to be run, and values are the environment variable values; e.g., MY_VAR1=my_val1,MY_VAR2=my_val2,... | None |
| --preemptible | No | If set, will request a preemptible VM rather than a standard one; the default behavior is to request standard VMs | False |


Here is an example command that will submit a "fastqc" task, which will use an open-access BAM file as input.  Before running this command, be sure to create at least one output destination path in GCS for your outputs and logs, and substitute those values in the command as directed:

```
isb-cgc-pipelines submit --pipelineName fastqc \
                --inputs gs://isb-cgc-open/ccle/BRCA/DNA-Seq/C836.MDA-MB-436.1.bam:C836.MDA-MB-436.1.bam,gs://isb-cgc-open/ccle/BRCA/DNA-Seq/C836.MDA-MB-436.1.bam.bai:C836.MDA-MB-436.1.bam.bai \
                --outputs gs://<YOUR_GCS_OUTPUT_PATH>:*_fastqc.zip,gs://<YOUR_GCS_OUTPUT_PATH>:*_fastqc.html \
                --cmd "fastqc C836.MDA-MB-436.1.bam" \
                --imageName b.gcr.io/isb-cgc-public-docker-images/fastqc \
                --cores 1 --mem 2 \
                --diskSize 100 \
                --logsPath gs://<YOUR_GCS_LOGS_PATH> \
                --preemptible
```

### Monitoring Tasks

To see the status of all tasks with a particular pipeline name at a glance, run the following command:

```
isb-cgc-pipelines list jobs --pipeline <pipelineName>
```

This will print the status of all pipelines with the name <pipelineName>.  For example, to see a listing of all submitted "fastqc" tasks, you would run the following command:

```
isb-cgc-pipelines list jobs --pipeline fastqc
```

Which would print something like this to the screen:

```
JOBID    PIPELINE    OPERATION ID                                                TAG                                     STATUS       CREATE TIME                 PREEMPTIONS
1        fastqc      EL7D18XQKhivqbDi1M_gyRUg3enn_9sIKg9wcm9kdWN0aW9uUXVldWU     b7b91877-b859-4c16-b592-a05c69ae500b    FAILED       2016-05-31T21:26:25.000Z    0
2        fastqc      EJD748XQKhixqoXym4ajisQBIN3p5__bCCoPcHJvZHVjdGlvblF1ZXVl    b7b91877-b859-4c16-b592-a05c69ae500b    FAILED       2016-05-31T21:29:49.000Z    0
3        fastqc      EOXK_cXQKhjenPSxjLiFznwg3enn_9sIKg9wcm9kdWN0aW9uUXVldWU     b7b91877-b859-4c16-b592-a05c69ae500b    PREEMTPED    2016-05-31T21:36:48.000Z    1
4        fastqc      ENSDisbQKhjW0I3FmIqLrDsg3enn_9sIKg9wcm9kdWN0aW9uUXVldWU     b7b91877-b859-4c16-b592-a05c69ae500b    FAILED       2016-05-31T21:40:12.000Z    0
5        fastqc      EJ3EosbQKhjX-OuP9ffxxuoBIN3p5__bCCoPcHJvZHVjdGlvblF1ZXVl    b7b91877-b859-4c16-b592-a05c69ae500b    FAILED       2016-05-31T21:46:54.000Z    0
6        fastqc      EOClu8bQKhibqKym8M68228g3enn_9sIKg9wcm9kdWN0aW9uUXVldWU     b7b91877-b859-4c16-b592-a05c69ae500b    SUCCEEDED    2016-05-31T21:53:40.000Z    0
```

If you'd like to print the stdout and stderr logs for a particular job to the console, choose a jobid from the first column of the listing of tasks and run the following command:

```
isb-cgc-pipelines logs --stdout --stderr <JOBID>
```
Another alternative way to find information about your submitted task is to query the operation status of the task directly using the `gcloud alpha genomics operations describe` command.  To do this, copy and paste the operation ID from the third column of the listing of tasks and run the following command:

```
gcloud alpha genomics operations describe <OPERATION_ID>
```

## Advanced Usage

This section covers some more advanced use cases, such as running a DAG or sequence of tasks.  For this particular use case, you can use the underlying API directly to create more complicated workflows.

### The PipelineBuilder API

More info coming soon!

## Future Plans

More info coming soon!

## Bug Reports

If you encounter any bugs, anomalous or unintended behavior, please feel free to submit any issues/feedback to the issue tracker in this repo.
