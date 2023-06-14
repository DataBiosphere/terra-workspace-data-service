package org.databiosphere.workspacedataservice.samplejob;

import com.maximeroussy.invitrode.WordGenerator;
import org.quartz.Job;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.stereotype.Component;

import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;

/*
    Does most of the work for our SampleJobs.
 */
@Component
public class SampleJob implements Job {

    private static final Logger LOGGER = LoggerFactory.getLogger(SampleJob.class);

    private final NamedParameterJdbcTemplate namedTemplate;

    public SampleJob(NamedParameterJdbcTemplate namedTemplate) {
        this.namedTemplate = namedTemplate;
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

        LOGGER.info("***** starting job " + jobId + " ...");

        namedTemplate.getJdbcTemplate().update("insert into sys_wds.samplejob(id) values(?) " +
                        "on conflict(id) do nothing",
                jobId.toString());

        try {
            doTheJob(context);
        } catch (Exception e) {
            // do what?
            throw e;
        }

        LOGGER.info("***** job " + jobId + " completed");
    }


    private void doTheJob(JobExecutionContext context) throws JobExecutionException {
        String jobId = context.getJobDetail().getKey().getName();
        long start = System.currentTimeMillis();

        // job starting ...


        int randomMillis = ThreadLocalRandom.current().nextInt(5000, 30000);
        try {
            Thread.sleep(randomMillis);
        } catch (InterruptedException e) {
            throw new JobExecutionException(e);
        }

        // to mock real behavior, throw exceptions sometimes.
        // jobs whose id starts with a number should succeed; those
        // that start with a letter will fail.
        if ("abcdef".contains(jobId.toString().substring(0, 1))) {
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
