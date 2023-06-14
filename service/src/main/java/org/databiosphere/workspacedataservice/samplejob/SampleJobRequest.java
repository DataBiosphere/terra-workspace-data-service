package org.databiosphere.workspacedataservice.samplejob;

import org.jobrunr.jobs.lambdas.JobRequest;

import java.util.UUID;

/**
 * JobRunr-required implementation of JobRequest for SampleJobs.
 * This doesn't do much other than pointing at the SampleJobRequestHandler.
 */
public class SampleJobRequest implements JobRequest {

//    private UUID jobId;
//
//    public void setJobId(UUID jobId) {
//        this.jobId = jobId;
//    }

//    public SampleJobRequest() {}

//    public SampleJobRequest(UUID jobId) {
//        this.jobId = jobId;
//    }

    @Override
    public Class<? extends SampleJobRequestHandler> getJobRequestHandler() {
        return SampleJobRequestHandler.class;
    }

//    public UUID getJobId() {
//        return jobId;
//    }

}
