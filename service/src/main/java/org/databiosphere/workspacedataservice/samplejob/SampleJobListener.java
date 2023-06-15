package org.databiosphere.workspacedataservice.samplejob;

import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;
import org.quartz.JobListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.stereotype.Component;

@Component
public class SampleJobListener implements JobListener {

    private static final Logger LOGGER = LoggerFactory.getLogger(SampleJobListener.class);
    private final NamedParameterJdbcTemplate namedTemplate;

    public SampleJobListener(NamedParameterJdbcTemplate namedTemplate) {
        this.namedTemplate = namedTemplate;
    }

    @Override
    public String getName() {
        return SampleJobListener.class.getSimpleName();
    }

    @Override
    public void jobToBeExecuted(JobExecutionContext context) {
        LOGGER.info("jobToBeExecuted: " + context.getJobDetail().getKey());
    }

    @Override
    public void jobExecutionVetoed(JobExecutionContext context) {
        LOGGER.info("jobExecutionVetoed: " + context.getJobDetail().getKey());
    }

    @Override
    public void jobWasExecuted(JobExecutionContext context, JobExecutionException jobException) {
        LOGGER.info("jobWasExecuted: " + context.getJobDetail().getKey());
    }
}
