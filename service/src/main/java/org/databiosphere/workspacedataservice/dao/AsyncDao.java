package org.databiosphere.workspacedataservice.dao;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.databiosphere.workspacedataservice.samplejob.SampleJob;
import org.databiosphere.workspacedataservice.samplejob.SampleJobListener;
import org.databiosphere.workspacedataservice.samplejob.SampleJobResponse;
import org.quartz.JobBuilder;
import org.quartz.JobDetail;
import org.quartz.JobExecutionContext;
import org.quartz.JobKey;
import org.quartz.Scheduler;
import org.quartz.SchedulerException;
import org.quartz.Trigger;
import org.quartz.TriggerBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.stereotype.Component;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.ZoneId;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

import static org.quartz.impl.matchers.EverythingMatcher.allJobs;

/**
 * Proof-of-concept DAO for working with async jobs
 */
@Component
public class AsyncDao {

    private static final Logger LOGGER = LoggerFactory.getLogger(AsyncDao.class);
    private final NamedParameterJdbcTemplate namedTemplate;
    private final ObjectMapper objectMapper;
    private final Scheduler scheduler; // the quartz scheduler

    private final String GROUPNAME = "sample-group";

    public AsyncDao(NamedParameterJdbcTemplate namedTemplate, ObjectMapper objectMapper, Scheduler scheduler) {
        this.namedTemplate = namedTemplate;
        this.objectMapper = objectMapper;
        this.scheduler = scheduler;

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
        LOGGER.info("attempting to queue job with id " + uuid + " ...");

        JobKey jobKey = new JobKey(uuid.toString(), GROUPNAME);

        // what to run
        JobDetail jobDetail = JobBuilder.newJob().ofType(SampleJob.class)
                .storeDurably()
                .withIdentity(jobKey)
                .withDescription("Invoke Sample Job service...")
                .build();

        // when to run it
//        Trigger trigger = TriggerBuilder.newTrigger().forJob(jobDetail)
//                .withIdentity("run-now")
//                .withDescription("Sample trigger")
//                .startNow()
//                .build();

        // run the job once, immediately
        scheduler.addJob(jobDetail, true);
        scheduler.triggerJob(jobKey);
        // job starting ...
//        namedTemplate.getJdbcTemplate().update("insert into sys_wds.samplejob(id) values (?)",
//                jobKey.getName());


//        scheduler.scheduleJob(jobDetail, trigger);

        LOGGER.info("... job " + uuid + " queued.");

        // create the response object
        SampleJobResponse job = new SampleJobResponse();
        job.setJobId(uuid.toString());
        return job;
    }


    /*
     *
     */
    public SampleJobResponse getJob(String jobId) throws JsonProcessingException, SchedulerException {
        MapSqlParameterSource params = new MapSqlParameterSource("jobId", jobId);

        JobKey jobKey = new JobKey(jobId, GROUPNAME); // name, group

        List<JobExecutionContext> currentJobs = scheduler.getCurrentlyExecutingJobs();
        Optional<JobExecutionContext> maybeJob = currentJobs.stream()
                .filter(jec -> jec.getJobDetail().getKey().equals(jobKey))
                .findFirst();

        if (maybeJob.isEmpty()) {
            throw new RuntimeException("job not found");
        }

        JobExecutionContext jobExecutionContext = maybeJob.get();

        SampleJobResponse response = new SampleJobResponse();
        response.setJobId(jobId);
        response.setStatus("???");
        response.setSignature("???");
        response.setDuration(Long.valueOf(jobExecutionContext.getJobRunTime()).intValue());
        response.setWord("???");
        response.setUpdated(jobExecutionContext.getFireTime().toInstant().atZone(ZoneId.systemDefault()).toLocalDateTime());
        response.setCreated(jobExecutionContext.getScheduledFireTime().toInstant().atZone(ZoneId.systemDefault()).toLocalDateTime());

        return response;

        // is this job done?
//        if (job.getStatus().equals(StateName.SUCCEEDED.name())) {
//            // job is done; now, query the job's dedicated table to get the results.
//            // note this is a WDS-controlled table, not a JobRunr table.
//            Integer duration = namedTemplate.queryForObject(
//                    "select duration from sys_wds.samplejob where id = :jobId",
//                    params, Integer.class);
//            String word = namedTemplate.queryForObject(
//                    "select word from sys_wds.samplejob where id = :jobId",
//                    params, String.class);
//
//            SampleJobResponse response = new SampleJobResponse();
//            response.setJobId(jobId);
//            response.setStatus(job.getStatus());
//            response.setSignature(job.getSignature());
//            response.setDuration(duration);
//            response.setWord(word);
//            response.setCreated(job.getCreated());
//            response.setUpdated(job.getUpdated());
//            return response;
//        } else if (job.getStatus().equals(StateName.FAILED.name())) {
//            // job failed. Retrieve the job history from the jobrunr table
//            String jobasjson = namedTemplate.queryForObject(
//                    "select jobasjson from sys_wds.jobrunr_jobs where id = :jobId",
//                    params, String.class);
//            // deserialize the json
//            JobDetails jobDetails = objectMapper.readValue(jobasjson, JobDetails.class);
//            // find the last failure (there may be multiple failures in the job history due to retries)
//            List<JobHistory> failures = jobDetails.jobHistory().stream()
//                    .filter(hist -> hist.state().equals(StateName.FAILED.name()))
//                    .toList();
//            JobHistory lastFailure = failures.get(failures.size() - 1);
//            // build a failure message
//            String failureMsg = lastFailure.exceptionType() + ": " + lastFailure.exceptionMessage();
//
//            SampleJobResponse response = new SampleJobResponse();
//            response.setJobId(jobId);
//            response.setStatus(job.getStatus());
//            response.setSignature(job.getSignature());
//            response.setFailure(failureMsg);
//            response.setCreated(job.getCreated());
//            response.setUpdated(job.getUpdated());
//            return response;
//        } else {
//            // SCHEDULED, ENQUEUED, PROCESSING, or DELETED (might need other handling for DELETED)
//            return job;
//        }
    }


    // rowmapper for retrieving SampleJobResponse objects from the db
    private static class AsyncJobRowMapper implements RowMapper<SampleJobResponse> {
        @Override
        public SampleJobResponse mapRow(ResultSet rs, int rowNum) throws SQLException {
            SampleJobResponse job = new SampleJobResponse();
            job.setJobId(rs.getString("id"));
            job.setStatus(rs.getString("state"));
            job.setSignature(rs.getString("jobsignature"));
            job.setCreated(rs.getTimestamp("createdat").toLocalDateTime());
            job.setUpdated(rs.getTimestamp("updatedat").toLocalDateTime());
            return job;
        }
    }

}
