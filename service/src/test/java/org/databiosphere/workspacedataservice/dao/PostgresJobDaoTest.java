package org.databiosphere.workspacedataservice.dao;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;

import org.databiosphere.workspacedataservice.generated.GenericJobServerModel;
import org.databiosphere.workspacedataservice.shared.model.job.Job;
import org.databiosphere.workspacedataservice.shared.model.job.JobInput;
import org.databiosphere.workspacedataservice.shared.model.job.JobResult;
import org.databiosphere.workspacedataservice.shared.model.job.JobType;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.test.annotation.DirtiesContext;

@DirtiesContext
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@SpringBootTest
public class PostgresJobDaoTest {

  // createJob
  // updateStatus x 3
  // getJob

  @Autowired NamedParameterJdbcTemplate namedTemplate;
  @Autowired PostgresJobDao jobDao;

  @AfterAll
  public void afterAll() {
    // cleanup: delete everything from the job table
    namedTemplate.getJdbcTemplate().update("delete from sys_wds.job;");
  }

  // TODO: create a job. Is it in the db with status=CREATED and the appropriate type and id?
  @Test
  public void createJob() {
    var testJobType = JobType.DATA_IMPORT;
    Job<JobInput, JobResult> testJob = Job.newJob(testJobType, JobInput.empty());
    jobDao.createJob(testJob);

    var params = new MapSqlParameterSource("jobId", testJob.getJobId().toString());
    params.addValue("type", testJobType.name());
    params.addValue("status", GenericJobServerModel.StatusEnum.CREATED.name());

    // after creating a job, there should be exactly one row with:
    // this jobId, the specified job type, and in status = CREATED,
    // created and updated timestamps being not null,
    // result and error and stacktrace are null
    // and input is {}
    assertDoesNotThrow(
        () ->
            namedTemplate.queryForObject(
                "select id from sys_wds.job where id = :jobId and type = :type and status = :status "
                    + "and created is not null and updated is not null "
                    + "and input = '{}'::jsonb "
                    + "and result is null and error is null and stacktrace is null",
                params,
                String.class),
        "PostgresJobDao.createJob() should create exactly one row");
  }

  // TODO: update status, is it properly set and the updated timestamp changes?

  // TODO: update status with an error message

  // TODO: update status with a stacktrace

  // TODO: update status with an error message and stacktrace

  // TODO: get job, does it deserialize correctly?

}
