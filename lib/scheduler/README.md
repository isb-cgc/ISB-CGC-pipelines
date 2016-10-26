# Scheduling and Monitoring System

The ISB-CGC-pipelines scheduling and monitoring system has three major components:
- Processes managed by [supervisord](http://supervisord.org/) ([pipelineJobScheduler](.pipelineJobScheduler), [pipelineJobCanceller](.pipelineJobCanceller), [receivePipelineVmLogs](.receivePipelineVmLogs))
- Local message queueing with [rabbitmq](https://www.rabbitmq.com/)
- [Cloud Pub/Sub](https://cloud.google.com/pubsub/docs/) messages

When the scheduler is started, all of the processes managed by supervisord are started, and will block until they have something to do.

When a job is submitted using the `isb-cgc-pipelines submit ...` command, a message containing the jobid and request body is sent to the wait queue managed by the rabbitmq server.  The [pipelineJobScheduler](.pipelineJobScheduler) process will automatically dequeue a message in the "WAIT_Q", attempt to submit the job request within the message to the Google Genomics Pipelines (GGP) API service, and then update the job database as appropriate.

Similarly, when a job is cancelled using the `isb-cgc-pipelines stop ...` command, a message containing the jobid for the job in question is sent to the cancel queue managed by the rabbitmq server.  The [pipelineJobCanceller](.pipelineJobCanceller) process dequeues messages from the "CANCEL_Q", attempts to cancel the associated GGP operation and then updates the jobs database as appropriate.

The [receivePipelineJobLogs](.receivePipelineJobLogs) process handles messages from Cloud Pub/Sub.  The messages from Cloud Pub/Sub, in this case, originate from [log sinks](https://cloud.google.com/logging/docs/api/tasks/exporting-logs) created for the purpose of collecting Google Compute Engine VM logs filtered by VM name ("ggp-*").  These logs are used by the scheduling system to determine the status of a particular job in order to avoid excessive polling of the Genomics Operations API, which can lead to frequent "queries-per-second" errors if not handled properly.  This process blocks until a message becomes available, at which time it will decide whether a job succeeded, failed or was preempted.  The job's status is updated in the database as appropriate, and if there are any child jobs (whose dependencies are all met) then those jobs will be added to the wait queue.
