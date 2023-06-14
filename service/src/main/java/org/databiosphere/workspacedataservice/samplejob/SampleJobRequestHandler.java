package org.databiosphere.workspacedataservice.samplejob;

import com.maximeroussy.invitrode.WordGenerator;
import org.databiosphere.workspacedataservice.dao.AsyncDao;
import org.jobrunr.jobs.annotations.Job;
import org.jobrunr.jobs.context.JobContext;
import org.jobrunr.jobs.lambdas.JobRequestHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.stereotype.Component;

import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;

@Component
public class SampleJobRequestHandler implements JobRequestHandler<SampleJobRequest> {

    private static final Logger LOGGER = LoggerFactory.getLogger(SampleJobRequestHandler.class);

    private final NamedParameterJdbcTemplate namedTemplate;

    public SampleJobRequestHandler(NamedParameterJdbcTemplate namedTemplate) {
        this.namedTemplate = namedTemplate;
    }

    /**
     * Placeholder method for some operation that will take a long time.
     * This placeholder just sleeps for a random time between 5 and 60 seconds,
     * writes the actual runtime to the database, then completes.
     *
     * @return
     * @throws InterruptedException
     */
    @Override
    @Job(name = "My sample JobRunr job", retries = 2)
    public void run(SampleJobRequest jobRequest) throws Exception {

        UUID jobId = jobContext().getJobId();

        LOGGER.info("***** starting job " + jobId + " ...");

        long start = System.currentTimeMillis();

        int randomMillis = ThreadLocalRandom.current().nextInt(5000, 60000);
        Thread.sleep(randomMillis);

        // to mock real behavior, throw exceptions sometimes.
        // jobs whose id starts with a number should succeed; those
        // that start with a letter will fail.
        if ("abcdef".contains(jobId.toString().substring(0,1))) {
            throw new RuntimeException("whoops, async job hit an exception.");
        }

        WordGenerator generator = new WordGenerator();
        String randomWord = generator.newWord(9);

        long duration = System.currentTimeMillis() - start;

        LOGGER.info("***** job " + jobId + " completed; duration was " + duration);

        namedTemplate.getJdbcTemplate().update("insert into sys_wds.samplejob(id, duration, word) values (?, ?, ?)",
                jobId, duration, randomWord);
    }

    @Override
    public JobContext jobContext() {
        return JobRequestHandler.super.jobContext();
    }
}
