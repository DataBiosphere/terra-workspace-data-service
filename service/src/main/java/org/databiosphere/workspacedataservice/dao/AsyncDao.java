package org.databiosphere.workspacedataservice.dao;

import org.databiosphere.workspacedataservice.samplejob.SampleJobRequest;
import org.databiosphere.workspacedataservice.service.AsyncService;
import org.databiosphere.workspacedataservice.service.model.SampleJob;
import org.databiosphere.workspacedataservice.shared.model.AttributeComparator;
import org.jobrunr.jobs.JobId;
import org.jobrunr.jobs.lambdas.JobRequest;
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
import java.util.concurrent.ThreadLocalRandom;

/**
 * Proof-of-concept DAO for working with async jobs
 */
@Component
public class AsyncDao {

    private static final Logger LOGGER = LoggerFactory.getLogger(AsyncDao.class);
    private final NamedParameterJdbcTemplate namedTemplate;

    public AsyncDao(NamedParameterJdbcTemplate namedTemplate) {
        this.namedTemplate = namedTemplate;
    }

    public String createJob() {
        UUID uuid = UUID.randomUUID();
        JobRequest jobRequest = new SampleJobRequest(uuid);

        LOGGER.info("attempting to queue job with id " + uuid + " ...");

        JobId jobId = BackgroundJobRequest.enqueue(uuid, jobRequest);

        LOGGER.info("... job queued, actual id is " + uuid);

        return jobId.toString();
    }


    /**
     * Boooo - JobRunr open-source version does NOT provide programmatic access to
     * job status or job results.
     *
     * It's pretty trivial to query the JobRunr tables directly, bypassing their API,
     * to get status of a job. That's what we're doing here.
     *
     * This returns an SampleJob object, which contains:
     *  - status: the job's current state as reported by JobRunr: PROCESSING, SUCCEEDED, etc.
     *  - signature: the Java method that was run by the job. We don't want to expose this to
     *      end users, but we could use it internally.
     *
     * @param jobId
     * @return
     */
    public SampleJob getJob(String jobId) {
        MapSqlParameterSource params = new MapSqlParameterSource("jobId", jobId);

        List<SampleJob> jobs = namedTemplate.query(
                "select state, jobsignature from sys_wds.jobrunr_jobs where id = :jobId",
                params, new AsyncJobRowMapper());

        if (jobs.size() != 1) {
            throw new RuntimeException("found " + jobs.size() + " jobs for specified id.");
        }

        SampleJob job = jobs.get(0);

        // is this job done?
        if (job.status().equals("SUCCEEDED")) {
            // job is done; now, query the job's dedicated table to get the result
            Integer duration = namedTemplate.queryForObject(
                    "select duration from sys_wds.samplejob where id = :jobId",
                    params, Integer.class);

            return new SampleJob(job.status(), job.signature(), duration);
        } else {
            // handle different states here, like FAILED
            return job;
        }
    }



    // rowmapper for retrieving SampleJob objects from the db
    private static class AsyncJobRowMapper implements RowMapper<SampleJob> {
        @Override
        public SampleJob mapRow(ResultSet rs, int rowNum) throws SQLException {
            return new SampleJob(rs.getString("state"),
                    rs.getString("jobsignature"),
                    null);
        }
    }


}
