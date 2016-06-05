'use strict';

var google = require('googleapis');

module.exports = {
    schedulePipelineJob: function(context, data) {
        // receives a job object
        // inserts job record into database
        // if the job's dependencies are met, start the job
        // update the job's record with the job id
        context.success();
    },

    pipelineJobStdoutLogs: function(context, data) {
        // receives a log message from a job and logs it to the console
        console.log(data.message[...]);
        context.success();
    },

    pipelineJobStderrLogs: function(context, data) {
        // receives a log message from a job and logs it to the console
        console.log(data.message[...]);
        context.success();
    },

    pipelineServerLogs: function(context, data) {
        // receives a log message from the pipeline service and logs it to the console

    },

    pipelineVmInsert: function(context, data) {
        // get the operation id from the vm description
        // update the associated job's record (vm-name, resource_id)
        context.success();
    },

	pipelineVmPreempted: function(context, data) {
		// update the job record for the job
		// UPDATE jobs SET current_status = 'PREEMPTED', preemptions = (preemptions + 1) WHERE 
		// vm-resource-id = data.message[...]
		context.success();
	},

	pipelineVmDelete: function(context, data) {
		// check the output of the pipeline operation and update the job record
		var pipelineOperation = data.message[""]

		// get application default credentials

		var genomics = google.genomics('v1beta2');

		var resp = genomics.operations.get(...);

		//  check the status and update the job record
		//  UPDATE jobs SET current_status = 'SUCCEEDED' WHERE 
		//  vm-resource-id = data.message[...]
		context.success();
	}
}
