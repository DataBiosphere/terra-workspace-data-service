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
        namedTemplate.getJdbcTemplate().update("insert into sys_wds.job(id, status) values (?, ?)" +
                        " on conflict(id) do update set status = ?",
                context.getJobDetail().getKey().getName(), "QUEUED", "QUEUED");
    }

    @Override
    public void jobExecutionVetoed(JobExecutionContext context) {
        LOGGER.info("jobExecutionVetoed: " + context.getJobDetail().getKey());
        namedTemplate.getJdbcTemplate().update("insert into sys_wds.job(id, status) values (?, ?)" +
                        " on conflict(id) do update set status = ?",
                context.getJobDetail().getKey().getName(), "VETOED", "VETOED");
    }

    @Override
    public void jobWasExecuted(JobExecutionContext context, JobExecutionException jobException) {
        LOGGER.info("jobWasExecuted: " + context.getJobDetail().getKey());
        if (jobException == null) {
            namedTemplate.getJdbcTemplate().update("insert into sys_wds.job(id, status) values (?, ?)" +
                            " on conflict(id) do update set status = ?",
                    context.getJobDetail().getKey().getName(), "SUCCEEDED", "SUCCEEDED");
        } else {
            namedTemplate.getJdbcTemplate().update("insert into sys_wds.job(id, status, error) values (?, ?, ?)" +
                            " on conflict(id) do update set status = ?, error = ?",
                    context.getJobDetail().getKey().getName(), jobException.getMessage(),
                    "FAILED", "FAILED", jobException.getMessage());
        }

    }
}
