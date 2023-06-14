package org.databiosphere.workspacedataservice.samplejob;

import com.maximeroussy.invitrode.WordGenerator;
import org.jobrunr.jobs.annotations.Job;
import org.jobrunr.jobs.context.JobContext;
import org.jobrunr.jobs.lambdas.JobRequestHandler;
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
public class SampleJobRequestHandler implements JobRequestHandler<SampleJobRequest> {

    private static final Logger LOGGER = LoggerFactory.getLogger(SampleJobRequestHandler.class);

    private final NamedParameterJdbcTemplate namedTemplate;

    public SampleJobRequestHandler(NamedParameterJdbcTemplate namedTemplate) {
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
    @Job(name = "My sample JobRunr job", retries = 2)
    public void run(SampleJobRequest jobRequest) throws Exception {

        UUID jobId = jobContext().getJobId();

        LOGGER.info("***** starting job " + jobId + " ...");

        long start = System.currentTimeMillis();

        int randomMillis = ThreadLocalRandom.current().nextInt(5000, 30000);
        Thread.sleep(randomMillis);

        // to mock real behavior, throw exceptions sometimes.
        // jobs whose id starts with a number should succeed; those
        // that start with a letter will fail.
        if ("abcdef".contains(jobId.toString().substring(0, 1))) {
            throw new RuntimeException("whoops, async job hit an exception.");
        }

        WordGenerator generator = new WordGenerator();
        String randomWord = generator.newWord(9);

        long duration = System.currentTimeMillis() - start;

        namedTemplate.getJdbcTemplate().update("insert into sys_wds.samplejob(id, duration, word) values (?, ?, ?)",
                jobId, duration, randomWord);

        LOGGER.info("***** job " + jobId + " completed; duration was " + duration);
    }

    @Override
    public JobContext jobContext() {
        return JobRequestHandler.super.jobContext();
    }
}
