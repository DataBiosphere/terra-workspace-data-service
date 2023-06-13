package org.databiosphere.workspacedataservice.service;

import org.databiosphere.workspacedataservice.dao.AsyncDao;
import org.databiosphere.workspacedataservice.samplejob.SampleJobRequest;
import org.databiosphere.workspacedataservice.service.model.SampleJob;
import org.jobrunr.jobs.JobId;
import org.jobrunr.jobs.lambdas.JobRequest;
import org.jobrunr.scheduling.BackgroundJobRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.util.UUID;

/**
 * Proof-of-concept service for asynchronous APIs
 */
@Service
public class AsyncService {

    private static final Logger LOGGER = LoggerFactory.getLogger(AsyncService.class);

    private final AsyncDao asyncDao;

    public AsyncService(AsyncDao asyncDao) {
        this.asyncDao = asyncDao;
    }

    // POC for starting an asynchronous job
    public String startAsyncJob() {
        return asyncDao.createJob();
    }

    // POC for getting status of an asynchronous job
    public SampleJob describeAsyncJob(String jobId) {
        return asyncDao.getJob(jobId);
    }



}
