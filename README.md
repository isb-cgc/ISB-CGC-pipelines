# ISB-CGC-pipelines

A framework for running bioinformatic workflows and pipelines at scale using the [Google Genomics Pipelines API](https://cloud.google.com/genomics/reference/rest/v1alpha2/pipelines) as the underlying task-runner.

## [Terminology Clarification](#terminology-clarification)

The use of the term "pipeline" in the context of this project is somewhat overloaded.  The name of the project, ISB-CGC-pipelines, was derived from the fact that the framework is built on top of the Google Genomics Pipelines API, which itself uses the the term "pipeline" somewhat ambiguously.  Within the context of the Google Genomics Pipelines API, a "pipeline" is simply a single task run within a Docker container.  What's more, here a "single task" could be taken to mean a single Linux command or a lengthy script which executes an entire "pipeline" in the traditional sense of the word and is completely context dependent due to the flexible nature of the technology.  

It is possible to build true (directed, acyclic) "pipelines" using the [advanced features](#advanced-usage) of the ISB-CGC-pipelines framework, which amounts to running Google Genomics Pipelines API tasks in sequence and/or in parallel with one another.  The important thing to keep in mind about the use of the term "pipeline" in this README is that it depends somewhat on what the term "pipeline" means to you as a developer and how you've chosen to design your code, whether you've decided to run an entire "pipeline" within a single Docker container or to string a number of individual "pipeline" tasks together in order to keep your usage of Docker fairly modular.  Both approaches have benefits and tradeoffs, and both are also possible using this framework.  

## [Prerequisites](#prerequisites)

In order to use the ISB-CGC-pipelines tools, the following requirements must be met:
- You must be an "Owner" or "Editor" of a Google Cloud Project.  Some initial configuration steps will require "Owner" status, but use of the command line tool and underlying API will only require "Editor" status.
- You must have the following APIs enabled in your Google Cloud Project: Google Compute Engine, Google Genomics, Google Cloud Pub/Sub, Google Cloud Logging (aka Stackdriver Logging)
- (Optional) Install the Google Cloud SDK, including "alpha" subcommands for the `gcloud` command.  This step isn't required if you plan to use a Google Compute Engine VM for running the tool.

## [Helpful Information](#helpful-information)

Understanding of the following technologies and topics will be essential for successfully developing pipelines that can be run using the ISB-CGC-pipelines framework, and it is recommended that you read the linked documentation if necessary before proceeding any further with these instructions:

- Docker ([Official Tutorials](https://docs.docker.com/engine/tutorials/))
- Google Compute Engine ([Official Documentation](https://cloud.google.com/compute/docs/))
- Google Container Registry ([Official Documentation](https://cloud.google.com/container-registry/docs/))

## [Service Account Configuration](#service-account-configuration)

Note: The steps in this section require "Owner" status on a Google Cloud Project.

Once you've enabled the APIs mentioned above, you will need to modify the permissions for two service accounts within your project: the default "compute" service account, which is created for you by default when you enable the Compute Engine API, and the "cloud-logs" service account, which you may need to create manually  in order to manage some logging tasks associated with your pipeline jobs. 

To determine whether or not the "cloud-logs" service account needs to be created, navigate to the ["IAM & Admin" section of the Cloud Console](https://console.cloud.google.com/iam-admin/projects), select the desired project from the list and check the member listing in that project for the name "cloud-logs@system.gserviceaccount.com".  If that account isn't listed, you can create it by clicking "Add Member" and entering the member name as "cloud-logs@system.gserviceaccount.com".  At this point, you can also select the roles (Project) "Editor" and "Pub/Sub Publisher" before clicking "Add".

Once both of the required service accounts have been created, they must be granted the roles given in the table below in the "IAM & Admin" section of the Cloud Console:

| Service Account Name | Required Roles |
| ------ | ------|
| xxxxxxxxxxxx-compute@developer.gserviceaccount.com | (Project) Editor, Logs Configuration Writer, Pub/Sub Admin | 
| cloud-logs@system.gserviceaccount.com | (Project) Editor, Pub/Sub Publisher |

To grant a role to an existing service account, simply click on the dropdown to the right of the particular service account name, check the roles that you wish to grant, and then click "Save".

## [Workstation Setup](#workstation-setup)

### Method 1: Cloud Installation

The easiest way to set up the ISB-CGC-pipelines framework is to use the tool's Compute Engine startup script to bootstrap a the workstation.  To do this, you can run the following command (or a variation thereof) to create the workstation with the appropriate scopes:

```
gcloud compute instances create my-pipeline-workstation --metadata startup-script-url=gs://isb-cgc-open/vm-startup-scripts/isb-cgc-pipelines-startup.sh --scopes cloud-platform
```

Once the instance is ready, you can ssh to it using the following command:

```
gcloud compute ssh my-pipeline-workstation
```

For more information about the `gcloud compute` command and all of its possible arguments, please refer to the [documentation](https://cloud.google.com/compute/docs/gcloud-compute/). 


### Method 2: Local Installation

To install the tool locally, you can run the same startup script used above on your local workstation by running the following commands:

```
# make sure git is installed

sudo apt-get update && sudo apt-get install git

# clone the repo

git clone https://github.com/isb-cgc/ISB-CGC-pipelines.git

# run the startup script

cd ISB-CGC-pipelines && sudo ./instance-startup.sh
```
Note that you must have root privileges on whatever system you choose to install the tool.  It is recommended to use Method #1 for installing the tool on a GCE VM if you don't have root access on your local machine.

## [Basic Usage](#basic-usage)

The most basic way to use the tool is to use the built-in command line utility, `isb-cgc-pipelines`.  The following sections will describe what the various subcommands do, but you can run `isb-cgc-pipelines -h` at any time to print usage information.

### [Configuration](#configuration)

First, update PYTHONPATH:

`export PYTHONPATH=$PYTHONPATH:/usr/local/ISB-CGC-pipelines/lib`

You will also need to add yourself to the `supervisor` user group in order to start and stop the job scheduler:

```
# run the following command, and then log out and log back in again for the change to take effect

sudo usermod -a -G supervisor $USER
```

To configure the framework, run the following command and follow the prompts:

`isb-cgc-pipelines config set all`

Most of the given prompts will provide a suitable default value that you can use by simply pressing enter at each prompt.  If you are unsure what to enter for a particular value, the recommended approach is to accept the default value proposed for the given item.  The only exception to this is the value for the GCP project id, which must be provided by you during the configuration process.

There is one last configuration step, which is to "bootstrap" the messaging system that underlies the job scheduling/monitoring system.  To initialize this process simply run `isb-cgc-pipelines bootstrap`, which should report a success message if the bootstrap process was successful.  

If you run into problems with the messaging bootstrap process, double check that you have set permissions appropriately for your service accounts (mentioned above in a previous section).

### [Starting and Stopping the Scheduler](#starting-and-stopping-the-scheduler)

To start the scheduler:

`isb-cgc-pipelines scheduler start`

To stop the scheduler:

`isb-cgc-pipelines scheduler stop`

### [Creating buckets for outputs and logs](#creating-buckets-for-outputs-and-logs)

Before you can run any jobs, you will need to make sure that you have read and write access to at least one Google Cloud Storage bucket for storing logs and outputs.  You can run the `gsutil mb` command to create a new bucket from the command line, or you can also use the "Create Bucket" button from the Cloud Storage section of the Cloud Console.

For more information about making buckets using `gsutil`, refer to the [official documentation](https://cloud.google.com/storage/docs/gsutil/commands/mb).

### [Submitting a Task](#submitting-a-task)

To submit a task, you can use the `submit` subcommand, which takes the following possible arguments:

| Argument | Required? | Description | Default |
| -------- | --------- | ----------- | ------------- |
| --pipelineName | Yes | A name for this particular pipeline (need not be unique) | None |
| --tag | No | An arbitrary identifier | Will be automatically generated using Python's uuid module (uuid4) |
| --logsPath | Yes | Relative path to a directory or bucket in Google Cloud Storage where log files will automatically be generated | None |
| --diskSize | No | The size of the data disk, in GB; if not provided, no disk will be used | None |
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

### [Monitoring Tasks](#monitoring-tasks)

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

## [Cancelling Jobs](#cancelling-jobs)

If you'd like to stop a job (or set of jobs), you can run one of the following variations of the `isb-cgc-pipelines stop` subcommand:

```
isb-cgc-pipelines stop --jobId <jobId> # stops a single job by id

isb-cgc-pipelines stop --pipeline <pipelineName>  # stops any job with the given name

isb-cgc-pipelines stop --tag <tag>  # stops any job with the given tag

```

If the job is in the "RUNNING" state, the job will be killed during processing.  If the job is still in the "WAITING" state, it will not be submitted in the future.

## [Handling Failures](#handling-failures)

There are two main categories of situations in which a submitted job will fail.  The most obvious case is the one where the job itself is specified incorrectly (input or output locations are invalid, the Docker image doesn't exist at the specified location, the container script contains a bug, etc.), in which case the monitoring system will report that the status of the job is "FAILED". 

The other situation arises if a VM is preempted, which is only relevant if you've set the "--preemptible" flag for the particular job.  In this situation, a job may be cut short (i.e., preempted) by Google Compute Engine for a variety of reasons, some of which may be unrelated to the way that the job was specified by the user.  In addition to the fact that preemptible VMs have a maximum lifetime of 24 hours, a VM may also be preempted at random or if it is attempting to use more resources than it requested.

The number of preemptions for any particular job will be listed in the far right-hand column of the output of the `isb-cgc-pipelines list jobs ...` command.  If you decided to set preempted jobs to restart automatically in the tool's configuration, this number will increase each time a job is preempted and subsequently restarted.  You can use this information to determine the appropriate course of action when jobs are preempted -- if a job is only preempted once before succeeding, you can probably assume that the VM was preempted at random.  However, if you notice that a job is frequently preempted, this may be a sign that the job is consistently attempting to use more resources than it requested and you may need to modify future submissions of that particular job in order for it to run to completion.

In either case (preemption or outright failure), you can modify the job request sent to the Google Genomics Pipelines API Service by hand by running `isb-cgc-pipelines edit --jobId <jobId>`.  This will open the request as a file in the default editor on your system if the $EDITOR variable is set, and will use `/usr/bin/nano` otherwise.  Once you've made your edits, saved and quit, the modified request will be ingested back into the jobs database so that future restarts of that job will use the modified request.  

In the case of a job that has been frequently preempted, you can either a) wait for the job's next preemption to see the change take effect, or b) stop and restart the job.  For jobs with a "FAILED" status, you will only need to restart the job by running `isb-cgc-pipelines restart --jobId <jobId>`.

## [Advanced Usage](#advanced-usage)

This section covers some more advanced use cases, such as running a DAG or sequence of tasks.  For this particular use case, you can use the underlying API directly to create more complicated workflows.

### [The PipelineBuilder API](#pipeline-builder-api)

There are three primary classes in the ISB-CGC-pipelines repo that can be used directly for building pipelines:

- [PipelinesConfig](#pipelines-config-class) - An object containing configuration information extracted from the tool’s global configuration file, which is created and maintained via the `isb-cgc-pipelines config set` command.
- [PipelineSchema](#pipeline-schema-class) - Used for building the schema for an individual pipeline “step” 
- [PipelineBuilder](#pipeline-builder-class) - Used for combining instances of PipelineSchema into a single entity, as well as starting the pipeline 

## [The PipelinesConfig Class](#pipelines-config-class)

An instance of PipelinesConfig is required in order to create instances of PipelineSchema and PipelineBuilder (explained in the sections below).  

The simplest way to create a functional instance of the class is as follows:

```
config = PipelinesConfig()
```

## [The PipelineSchema Class](#pipeline-schema-class)

The PipelineSchema class is used to create an instance of a single pipeline task which will be run using the Google Genomics Pipelines service.  

To create an instance of the class, create an instance of PipelinesConfig and pass it to the constructor of PipelineSchema, along with a few additional parameters:

```
config = PipelinesConfig()
stepName = "stepA"
logsPath = "gs://my-pipeline-logs/"
imageName = "gcr.io/my-project-id/my-docker-image"
stepA = PipelineSchema(pipelineName, logsPath, imageName, config)
```

The constructor for PipelineSchema has the following signature:

```
PipelineSchema(name, config, logsPath, imageName, scriptUrl=None, cmd=None, cores=1, mem=1, diskSize=None,
	             diskType=None, env=None, inputs=None, outputs=None, tag=None, children=None, metadata=None,
	             preemptible=False)
```
	             
You can specify all of the components of a particular pipeline step in one line using the keyword arguments above or, alternatively, you can use the methods below to add things individually, which may make your code more readable and easier to maintain.

### Methods
Once you've created a PipelineSchema instance, the following methods can be used to specify various parts of the API request sent to that service.  Some methods are required to be run in order for the request to succeed (as noted below), but they can be run in any order.

#### getSchema()
  
Parameters:
  - None

Description:
  - Returns the JSON formatted pipeline request; *not required*

#### addChild(childStepName)

Parameters:  
  - **childStepName**: string; the name of the child job to add

Description:
  - Adds a child by name; *not required*

#### addInput(name, disk, localPath, gcsPath)

Parameters:
  - **name**:  string; a descriptive name for the input, e.g. "bam-input"
  - **disk**:  string; the name of the disk used to store inputs
  - **localPath**:  string; the relative path on the disk to store the file
  - **gcsPath**:  string; the fully-qualified GCS path to the input file to download to the disk

Description:
  - Adds a named input parameter to the job; *not required*

#### addOutput(name, disk, localPath, gcsPath)

Parameters:
  - **name**:  string; a descriptive name for the output, e.g. "bai-output"
  - **disk**:  string; the name of the disk where the outputs will be located when the processing has finished
  - **localPath**:  string; the relative path on the disk to the output file(s) to upload (accepts wildcards)
  - **gcsPath**:  string; the fully-qualified GCS path to the destination directory or filename for the output 

Description:
  - Adds a named output parameter to the job; *not required*

#### addDisk(name, diskType, sizeGb, mountPath, autoDelete=True, readOnly=False)

Parameters:
  - **name**:  string; a descriptive name for the disk, e.g. "samtools"
  - **diskType**:  string; one of "PERSISTENT_HDD" or "PERSISTEND_SSD"
  - **sizeGb**:  integer; the size of the disk in gigabytes
  - **mountPath**:  string; the container mount path of the disk
  - **autoDelete**:  boolean; whether or not to delete the disk when the job finishes; default = True
  - **readOnly**:  boolean; whether or not to mount the disk in read-only mode; default = False

Description:
  - Adds a persistent disk to the job; *required*

#### setMem(memGb)

Parameters:
  - **memGb**:  integer; the amount of RAM in gigabytes

Description:
  - sets the amount of RAM in gigabytes -- if unset, a default of 2GB will be used; *not required*

#### setCpu(cores)

Parameters:
  - **cores**:  integer; the number of cores to request

Description:
  - sets the number of cores for a job -- if unset, a default of 1 core will be used; *not required*

#### setPreemptible()

Parameters:
  - None

Description:
  - sets "preemptible" to True -- if unset, the job will use a standard non-preemptible VM by default; *not required*

#### setCmd(cmdString)

Parameters:
  - **cmdString**:  string; a single line command to run within the job's Docker container

Description:
  - sets the job's container command; *not required, but strongly recommended*


## [The PipelineBuilder Class](#pipeline-builder-class)

Instances of the PipelineBuilder class are used to collect all of the pipeline steps together into a single entity.  Creating an instance of this class is similar to creating instances of PipelineSchema:

```
config = PipelinesConfig()
pipeline = PipelineBuilder(config)
```

Combining steps into a single pipeline is as simple as running the following method for all of the steps that you've created:

```
pipeline.addStep(stepName)
```

**Note that all pipeline steps must have unique names in order for the pipeline to be properly validated!**

### Methods

#### addStep(step)

Parameters:
  - **step**:  PipelineSchema; an instance of PipelineSchema to add as a step

Description:
  - adds a step to the pipeline; *at least one step is required*

#### hasStep(stepName)

Parameters:
  - **stepName**: string; the name of the step that you're checking for the existence of

Description:
  - returns True if the pipeline contains a step with the given name; *not required*

#### run()

Parameters:
  - None

Description:
  - starts the pipeline; *required*

## [DAG Workflow Example](#dag-workflow-example)

Below is a short code skeleton demonstrating at a high level how to use the underlying API directly to build a simple two-step pipeline:

```
from utils import PipelinesConfig, PipelineSchema, PipelineBuilder

# create config, pipeline objects
config = PipelinesConfig()
pipeline = PipelineBuilder(config)

# additional info
logOutput = "gs://my-log-output"
stepAimg = "gcr.io/my-project-id/stepA"
stepBimg = "gcr.io/my-project-id/stepB"

# create the first step
stepA = PipelineSchema(‘stepA’,  config, logsPath, stepAimg, tag=‘stepA-tag’)

stepA.addDisk(...)
stepA.addInput(...)
stepA.addOutput(...)
stepA.setMem(...)
stepA.setCpu(...)
stepA.setCmd(...)
stepA.setPreemptible()

# create the second step
stepB = PipelineSchema(‘stepB’,  config, logsPath, stepBimg, tag=‘stepB-tag’)

stepB.addDisk(...)
stepB.addInput(...)
stepB.addOutput(...)
stepB.setMem(...)
stepB.setCpu(...)
stepB.setCmd(...)
stepB.setPreemptible()

# chain the steps together
stepA.addChild(stepB)
pipeline.addStep(stepA)
pipeline.addStep(stepB)

# run the pipeline
pipeline.run()
```

A real world example can be found [here](https://raw.githubusercontent.com/isb-cgc/ISB-CGC-pipelines/master/lib/examples/api/incoming-data.py).

Consult the [README](https://github.com/isb-cgc/ISB-CGC-pipelines/tree/master/lib/examples) in the examples directory for additional usage examples.

## [Future Plans](#future-plans)

More info coming soon!

## [Bug Reports](#bug-reports)

If you encounter any bugs, anomalous or unintended behavior, please feel free to submit any issues/feedback to the issue tracker in this repo.
