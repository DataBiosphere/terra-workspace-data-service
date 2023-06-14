package org.databiosphere.workspacedataservice.dao;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.databiosphere.workspacedataservice.samplejob.SampleJobRequest;
import org.databiosphere.workspacedataservice.service.model.JobDetails;
import org.databiosphere.workspacedataservice.service.model.JobHistory;
import org.databiosphere.workspacedataservice.service.model.SampleJob;
import org.jobrunr.jobs.JobId;
import org.jobrunr.jobs.lambdas.JobRequest;
import org.jobrunr.jobs.states.StateName;
import org.jobrunr.scheduling.BackgroundJobRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.stereotype.Component;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;
import java.util.UUID;

/**
 * Proof-of-concept DAO for working with async jobs
 */
@Component
public class AsyncDao {

    private static final Logger LOGGER = LoggerFactory.getLogger(AsyncDao.class);
    private final NamedParameterJdbcTemplate namedTemplate;
    private final ObjectMapper objectMapper;

    public AsyncDao(NamedParameterJdbcTemplate namedTemplate, ObjectMapper objectMapper) {
        this.namedTemplate = namedTemplate;
        this.objectMapper = objectMapper;
    }

    /*
        Enqueues a new "SampleJob" asynchronous job and returns that job's id.
     */
    public SampleJob createJob() {
        UUID uuid = UUID.randomUUID();
        LOGGER.info("attempting to queue job with id " + uuid + " ...");

        // simplest JobRunr syntax uses a lambda:
        // BackgroundJobRequest.enqueue(() -> someClass.doSomething());

        // but in our case, we need the "doSomething()" method to have access
        // to the job's id. Therefore, we use the more complex but more powerful
        // JobRequest and JobRequestHandler interfaces.
        JobRequest jobRequest = new SampleJobRequest();
        JobId jobId = BackgroundJobRequest.enqueue(uuid, jobRequest);

        LOGGER.info("... job " + uuid + " queued.");

        // create the response object
        SampleJob job = new SampleJob();
        job.setJobId(jobId.toString());
        return job;
    }


    /*
     * Boooo - JobRunr open-source version does NOT provide programmatic access to
     * job status or job results. We need JobRunr Pro for that ($$$).
     *
     * It's pretty trivial to query the JobRunr tables directly, bypassing their API,
     * to get status of a job. That's what we're doing here.
     */
    public SampleJob getJob(String jobId) throws JsonProcessingException {
        MapSqlParameterSource params = new MapSqlParameterSource("jobId", jobId);

        List<SampleJob> jobs = namedTemplate.query(
                "select id, state, jobsignature, createdat, updatedat " +
                        "from sys_wds.jobrunr_jobs where id = :jobId",
                params, new AsyncJobRowMapper());

        if (jobs.size() != 1) {
            throw new RuntimeException("found " + jobs.size() + " jobs for specified id.");
        }
        SampleJob job = jobs.get(0);

        // is this job done?
        if (job.getStatus().equals(StateName.SUCCEEDED.name())) {
            // job is done; now, query the job's dedicated table to get the results.
            // note this is a WDS-controlled table, not a JobRunr table.
            Integer duration = namedTemplate.queryForObject(
                    "select duration from sys_wds.samplejob where id = :jobId",
                    params, Integer.class);
            String word = namedTemplate.queryForObject(
                    "select word from sys_wds.samplejob where id = :jobId",
                    params, String.class);

            SampleJob response = new SampleJob();
            response.setJobId(jobId);
            response.setStatus(job.getStatus());
            response.setSignature(job.getSignature());
            response.setDuration(duration);
            response.setWord(word);
            response.setCreated(job.getCreated());
            response.setUpdated(job.getUpdated());
            return response;
        } else if (job.getStatus().equals(StateName.FAILED.name())) {
            // job failed. Retrieve the job history from the jobrunr table
            String jobasjson = namedTemplate.queryForObject(
                    "select jobasjson from sys_wds.jobrunr_jobs where id = :jobId",
                    params, String.class);
            // deserialize the json
            JobDetails jobDetails = objectMapper.readValue(jobasjson, JobDetails.class);
            // find the last failure (there may be multiple failures in the job history due to retries)
            List<JobHistory> failures = jobDetails.jobHistory().stream()
                    .filter(hist -> hist.state().equals(StateName.FAILED.name()))
                    .toList();
            JobHistory lastFailure = failures.get(failures.size() - 1);
            // build a failure message
            String failureMsg = lastFailure.exceptionType() + ": " + lastFailure.exceptionMessage();

            SampleJob response = new SampleJob();
            response.setJobId(jobId);
            response.setStatus(job.getStatus());
            response.setSignature(job.getSignature());
            response.setFailure(failureMsg);
            response.setCreated(job.getCreated());
            response.setUpdated(job.getUpdated());
            return response;
        } else {
            // SCHEDULED, ENQUEUED, PROCESSING, or DELETED (might need other handling for DELETED)
            return job;
        }
    }


    // rowmapper for retrieving SampleJob objects from the db
    private static class AsyncJobRowMapper implements RowMapper<SampleJob> {
        @Override
        public SampleJob mapRow(ResultSet rs, int rowNum) throws SQLException {
            SampleJob job = new SampleJob();
            job.setJobId(rs.getString("id"));
            job.setStatus(rs.getString("state"));
            job.setSignature(rs.getString("jobsignature"));
            job.setCreated(rs.getTimestamp("createdat").toLocalDateTime());
            job.setUpdated(rs.getTimestamp("updatedat").toLocalDateTime());
            return job;
        }
    }

}
