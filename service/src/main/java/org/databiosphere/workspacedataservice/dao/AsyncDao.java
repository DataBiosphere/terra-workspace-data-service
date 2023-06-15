package org.databiosphere.workspacedataservice.dao;

import com.fasterxml.jackson.core.JsonProcessingException;
import org.databiosphere.workspacedataservice.samplejob.SampleJob;
import org.databiosphere.workspacedataservice.samplejob.SampleJobListener;
import org.databiosphere.workspacedataservice.samplejob.SampleJobResponse;
import org.quartz.JobBuilder;
import org.quartz.JobDetail;
import org.quartz.JobKey;
import org.quartz.Scheduler;
import org.quartz.SchedulerException;
import org.quartz.Trigger;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.stereotype.Component;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.List;
import java.util.UUID;

import static org.quartz.TriggerBuilder.newTrigger;
import static org.quartz.impl.matchers.EverythingMatcher.allJobs;

/**
 * Proof-of-concept DAO for working with async jobs
 */
@Component
public class AsyncDao {

    private static final Logger LOGGER = LoggerFactory.getLogger(AsyncDao.class);
    private final NamedParameterJdbcTemplate namedTemplate;
    private final Scheduler scheduler; // the quartz scheduler

    // not super necessary, since we're using unique ids for job names,
    // but we could use the GROUPNAME to easily distinguish different types of
    // async jobs: PFB import vs. backup, etc.
    private final String GROUPNAME = "sample-group";

    public AsyncDao(NamedParameterJdbcTemplate namedTemplate, Scheduler scheduler) {
        this.namedTemplate = namedTemplate;
        this.scheduler = scheduler;
        // register the SampleJobListener which provides additional logging. This listener isn't really necessary,
        // but I wanted to explore the technique in case we need it for future use cases.
        try {
            this.scheduler.getListenerManager().addJobListener(new SampleJobListener(namedTemplate), allJobs());
        } catch (SchedulerException e) {
            LOGGER.error("error registering listener: " + e.getMessage(), e);
        }
    }

    /*
        Enqueues a new "SampleJobResponse" asynchronous job and returns that job's id.
     */
    public SampleJobResponse createJob() throws SchedulerException {
        UUID uuid = UUID.randomUUID();
        JobKey jobKey = new JobKey(uuid.toString(), GROUPNAME);
        LOGGER.info("attempting to queue job with id " + uuid + " ...");

        // what to run
        JobDetail jobDetail = JobBuilder.newJob()
                .ofType(SampleJob.class)
                .withIdentity(jobKey)
                .storeDurably(false) // delete from the quartz table after the job finishes
                .withDescription("Invoke Sample Job service...")
                .build();

        // create the db entry to track this job
        namedTemplate.getJdbcTemplate().update(
                "insert into sys_wds.samplejob(id, status, createdat) values (?, ?, ?)",
                jobKey.getName(),
                "CREATED",
                Timestamp.from(Instant.now()));

        // run the job once, immediately
        Trigger trigger = newTrigger()
                .withIdentity(uuid.toString())
                .forJob(jobKey)
                .startNow()
                .build();
        scheduler.scheduleJob(jobDetail, trigger);
        updateStatus(jobKey.getName(), "QUEUED");

        // create the response object
        SampleJobResponse job = new SampleJobResponse();
        job.setJobId(jobKey.getName());
        return job;
    }


    /*
     * Retrieve the row from our custom samplejob table.
     */
    public SampleJobResponse getJob(String jobId) throws JsonProcessingException, SchedulerException {

        MapSqlParameterSource params = new MapSqlParameterSource("jobId", jobId);
        List<SampleJobResponse> jobs = namedTemplate.query(
                "select id, duration, word, status, createdat, updatedat, error " +
                        "from sys_wds.samplejob where id = :jobId",
                params, new AsyncJobRowMapper());

        if (jobs.size() != 1) {
            throw new RuntimeException("found " + jobs.size() + " jobs for specified id.");
        }
        SampleJobResponse job = jobs.get(0);

        return job;
    }

    /*
     * Update the job's status
     */
    public void updateStatus(String jobId, String newStatus) {
        namedTemplate.getJdbcTemplate().update(
                "update sys_wds.samplejob set status = ?, updatedat = ? where id = ?",
                newStatus,
                Timestamp.from(Instant.now()),
                jobId
        );
        LOGGER.info("***** job " + jobId + " is now " + newStatus);
    }

    // rowmapper for retrieving SampleJobResponse objects from the db
    private static class AsyncJobRowMapper implements RowMapper<SampleJobResponse> {
        @Override
        public SampleJobResponse mapRow(ResultSet rs, int rowNum) throws SQLException {
            SampleJobResponse job = new SampleJobResponse();
            job.setJobId(rs.getString("id"));
            job.setDuration(rs.getInt("duration"));
            job.setWord(rs.getString("word"));
            job.setStatus(rs.getString("status"));
            job.setCreated(rs.getTimestamp("createdat").toLocalDateTime());
            job.setUpdated(rs.getTimestamp("updatedat").toLocalDateTime());
            job.setFailure(rs.getString("error"));
            return job;
        }
    }

}
