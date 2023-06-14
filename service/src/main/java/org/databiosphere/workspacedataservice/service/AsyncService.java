package org.databiosphere.workspacedataservice.service;

import com.fasterxml.jackson.core.JsonProcessingException;
import org.databiosphere.workspacedataservice.dao.AsyncDao;
import org.databiosphere.workspacedataservice.samplejob.SampleJobResponse;
import org.quartz.SchedulerException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

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
    public SampleJobResponse startAsyncJob() throws SchedulerException {
        return asyncDao.createJob();
    }

    // POC for getting status of an asynchronous job
    public SampleJobResponse describeAsyncJob(String jobId) throws JsonProcessingException, SchedulerException {
        return asyncDao.getJob(jobId);
    }


}
