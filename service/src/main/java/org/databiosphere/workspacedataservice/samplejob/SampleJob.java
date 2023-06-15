package org.databiosphere.workspacedataservice.samplejob;

import com.maximeroussy.invitrode.WordGenerator;
import org.databiosphere.workspacedataservice.dao.AsyncDao;
import org.quartz.Job;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.stereotype.Component;

import java.util.concurrent.ThreadLocalRandom;

/*
    Does most of the work for our SampleJobs.
 */
@Component
public class SampleJob implements Job {

    private static final Logger LOGGER = LoggerFactory.getLogger(SampleJob.class);

    private final NamedParameterJdbcTemplate namedTemplate;

    private final AsyncDao asyncDao;

    public SampleJob(NamedParameterJdbcTemplate namedTemplate, AsyncDao asyncDao) {
        this.namedTemplate = namedTemplate;
        this.asyncDao = asyncDao;
    }

    /*
     * Placeholder method for some operation that will take a long time.
     * This mock method does the following:
     *      - sleep for a random amount of time between 5 and 30 seconds
     *      - throw errors for roughly half of the requests, to test error-handling
     *      - when successful, generate a random word and save that word to the db.
     */
    @Override
    @SuppressWarnings("java:S2245")
    public void execute(JobExecutionContext context) throws JobExecutionException {
        String jobId = context.getJobDetail().getKey().getName();

        asyncDao.updateStatus(jobId, "STARTED");

        try {
            doTheJob(context);
            asyncDao.updateStatus(jobId, "SUCCEEDED");
        } catch (Exception e) {
            LOGGER.error("job hit an error" + e.getMessage(), e);
            namedTemplate.getJdbcTemplate().update(
                    "update sys_wds.samplejob set error = ? where id = ?",
                    e.getClass().getSimpleName() + ": " + e.getMessage(),
                    jobId
            );
            asyncDao.updateStatus(jobId, "FAILED");
            // throw with refireImmediately = false
            throw new JobExecutionException("error in job" + e.getMessage(), e, false);
        }
    }

    private void doTheJob(JobExecutionContext context) throws JobExecutionException {
        String jobId = context.getJobDetail().getKey().getName();
        long start = System.currentTimeMillis();

        // job starting ...
        asyncDao.updateStatus(jobId, "RUNNING");

        int randomMillis = ThreadLocalRandom.current().nextInt(5000, 30000);
        try {
            Thread.sleep(randomMillis);
        } catch (InterruptedException e) {
            throw new JobExecutionException(e);
        }

        // to mock real behavior, throw exceptions sometimes.
        // jobs whose id starts with a number should succeed; those
        // that start with a letter will fail.
        if ("abcdef".contains(jobId.substring(0, 1))) {
            throw new RuntimeException("whoops, async job hit an exception.");
        }

        WordGenerator generator = new WordGenerator();
        String randomWord = generator.newWord(9);

        long duration = System.currentTimeMillis() - start;

        namedTemplate.getJdbcTemplate().update("update sys_wds.samplejob " +
                        " set duration = ?, word = ?" +
                        " where id = ?",
                duration, randomWord, jobId);
    }

}
