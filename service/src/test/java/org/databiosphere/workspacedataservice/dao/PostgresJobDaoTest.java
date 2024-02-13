package org.databiosphere.workspacedataservice.dao;

import static org.databiosphere.workspacedataservice.generated.GenericJobServerModel.JobTypeEnum;
import static org.databiosphere.workspacedataservice.generated.GenericJobServerModel.StatusEnum;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.UUID;
import org.databiosphere.workspacedataservice.generated.GenericJobServerModel;
import org.databiosphere.workspacedataservice.shared.model.CollectionId;
import org.databiosphere.workspacedataservice.shared.model.job.Job;
import org.databiosphere.workspacedataservice.shared.model.job.JobInput;
import org.databiosphere.workspacedataservice.shared.model.job.JobResult;
import org.databiosphere.workspacedataservice.shared.model.job.JobType;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.test.annotation.DirtiesContext;

@DirtiesContext
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@SpringBootTest
class PostgresJobDaoTest {

  // createJob
  // updateStatus x 3
  // getJob

  @Autowired NamedParameterJdbcTemplate namedTemplate;
  @Autowired PostgresJobDao jobDao;
  @Autowired ObjectMapper mapper;

  @AfterAll
  void afterAll() {
    // cleanup: delete everything from the job table
    namedTemplate.getJdbcTemplate().update("delete from sys_wds.job;");
  }

  // helper method to get a job into the db and verify it was written correctly
  private GenericJobServerModel assertJobCreation(JobType jobType) {
    CollectionId collectionId = CollectionId.of(UUID.randomUUID());
    Job<JobInput, JobResult> testJob = Job.newJob(collectionId, jobType, JobInput.empty());
    jobDao.createJob(testJob);

    var params = new MapSqlParameterSource("jobId", testJob.getJobId().toString());
    params.addValue("type", jobType.name());
    params.addValue("collectionId", collectionId.id());
    params.addValue("status", StatusEnum.CREATED.name());

    // after creating a job, there should be exactly one row with:
    // this jobId, the specified job type, and in status = CREATED,
    // created and updated timestamps being not null,
    // result and error and stacktrace are null
    // and input is {}
    assertDoesNotThrow(
        () ->
            namedTemplate.queryForObject(
                "select id from sys_wds.job where id = :jobId and type = :type and status = :status "
                    + "and instance_id = :collectionId " // TODO: instance_id will be changed to
                    // collection_id in a later PR for AJ-1592
                    + "and created is not null and updated is not null "
                    + "and input = '{}'::jsonb "
                    + "and result is null and error is null and stacktrace is null",
                params,
                String.class),
        "PostgresJobDao.createJob() should create exactly one row");

    // return the actual job from the db; this ensures the created and updated timestamps are
    // correct.
    // note this means we are effectively asserting on getJob() in this helper method too
    return assertDoesNotThrow(() -> jobDao.getJob(testJob.getJobId()));
  }

  // create a Job
  @ParameterizedTest(name = "Create a job with type {0}")
  @EnumSource(JobType.class)
  void createJob(JobType jobType) {
    assertJobCreation(jobType);
  }

  // update status, is it properly set and the updated timestamp changes?
  @ParameterizedTest(name = "Reject job updates to status {0}")
  @EnumSource(
      value = StatusEnum.class,
      names = {"UNKNOWN", "ERROR"},
      mode = EnumSource.Mode.INCLUDE)
  void rejectUpdate(StatusEnum status) {
    JobType jobType = JobType.DATA_IMPORT;
    GenericJobServerModel testJob = assertJobCreation(jobType);

    assertThrows(
        IllegalArgumentException.class, () -> jobDao.updateStatus(testJob.getJobId(), status));
  }

  // update status, is it properly set and the updated timestamp changes?
  @ParameterizedTest(name = "Update a job to status {0}")
  @EnumSource(
      value = StatusEnum.class,
      names = {"UNKNOWN", "ERROR"},
      mode = EnumSource.Mode.EXCLUDE)
  void update(StatusEnum status) {
    JobType jobType = JobType.DATA_IMPORT;
    GenericJobServerModel testJob = assertJobCreation(jobType);
    jobDao.updateStatus(testJob.getJobId(), status);

    // after updating the job, there should be exactly one row with:
    // this jobId and type, the new status,
    // and updated timestamp greater than the created timestamp.
    // input should still be {}, and result, error, and stacktrace should still be null
    var params = new MapSqlParameterSource("jobId", testJob.getJobId().toString());
    params.addValue("type", jobType.name());
    params.addValue("status", status.name());
    assertDoesNotThrow(
        () ->
            namedTemplate.queryForObject(
                "select id from sys_wds.job where id = :jobId and type = :type and status = :status "
                    + "and updated > created "
                    + "and input = '{}'::jsonb "
                    + "and result is null and error is null and stacktrace is null",
                params,
                String.class),
        "should properly update the job");
  }

  // fail the job with an error message
  @Test
  void failWithErrorMessage() {
    JobType jobType = JobType.DATA_IMPORT;
    String errorMessage = "my unit test error message";
    GenericJobServerModel testJob = assertJobCreation(jobType);
    jobDao.fail(testJob.getJobId(), errorMessage);

    // after updating the job, there should be exactly one row with:
    // this jobId and type, the new status, and the new error message
    // and updated timestamp greater than the created timestamp.
    // input should still be {}, and result, and stacktrace should still be null
    var params = new MapSqlParameterSource("jobId", testJob.getJobId().toString());
    params.addValue("type", jobType.name());
    params.addValue("status", StatusEnum.ERROR.name());
    params.addValue("error", errorMessage);
    assertDoesNotThrow(
        () ->
            namedTemplate.queryForObject(
                "select id from sys_wds.job where id = :jobId and type = :type and status = :status "
                    + "and error = :error "
                    + "and updated > created "
                    + "and input = '{}'::jsonb "
                    + "and result is null and stacktrace is null",
                params,
                String.class),
        "should properly update the job with an error message");
  }

  // fail the job with an exception
  @Test
  void failWithException() throws JsonProcessingException {
    JobType jobType = JobType.DATA_IMPORT;
    String errorMessage = "my stack trace error message";
    StackTraceElement[] stackTrace = Thread.currentThread().getStackTrace();
    Exception e = new Exception(errorMessage);
    e.setStackTrace(stackTrace);

    GenericJobServerModel testJob = assertJobCreation(jobType);
    jobDao.fail(testJob.getJobId(), e);

    // after updating the job, there should be exactly one row with:
    // this jobId and type, the new status, the new error message, and the new stack trace
    // and updated timestamp greater than the created timestamp.
    // input should still be {}, and result should still be null
    var params = new MapSqlParameterSource("jobId", testJob.getJobId().toString());
    params.addValue("type", jobType.name());
    params.addValue("status", StatusEnum.ERROR.name());
    params.addValue("error", errorMessage);
    params.addValue("stacktrace", mapper.writeValueAsString(stackTrace));
    assertDoesNotThrow(
        () ->
            namedTemplate.queryForObject(
                "select id from sys_wds.job where id = :jobId and type = :type and status = :status "
                    + "and error = :error "
                    + "and stacktrace = :stacktrace::jsonb "
                    + "and updated > created "
                    + "and input = '{}'::jsonb "
                    + "and result is null",
                params,
                String.class),
        "should properly update the job with an error message and a stack trace");
  }

  // fail the job with a custom error message and an exception
  @Test
  void failWithExceptionAndMessage() throws JsonProcessingException {
    JobType jobType = JobType.DATA_IMPORT;
    StackTraceElement[] stackTrace = Thread.currentThread().getStackTrace();
    Exception e = new Exception("The exception's error message");
    e.setStackTrace(stackTrace);

    String customErrorMessage = "my override of the error message";

    GenericJobServerModel testJob = assertJobCreation(jobType);
    jobDao.fail(testJob.getJobId(), customErrorMessage, e);

    // after updating the job, there should be exactly one row with:
    // this jobId and type, the new status, the new error message, and the new stack trace
    // and updated timestamp greater than the created timestamp.
    // input should still be {}, and result should still be null
    var params = new MapSqlParameterSource("jobId", testJob.getJobId().toString());
    params.addValue("type", jobType.name());
    params.addValue("status", StatusEnum.ERROR.name());
    params.addValue("error", customErrorMessage);
    params.addValue("stacktrace", mapper.writeValueAsString(stackTrace));
    assertDoesNotThrow(
        () ->
            namedTemplate.queryForObject(
                "select id from sys_wds.job where id = :jobId and type = :type and status = :status "
                    + "and error = :error "
                    + "and stacktrace = :stacktrace::jsonb "
                    + "and updated > created "
                    + "and input = '{}'::jsonb "
                    + "and result is null",
                params,
                String.class),
        "should properly update the job with an error message and a stack trace");
  }

  @Test
  void queue() {
    JobType jobType = JobType.DATA_IMPORT;
    GenericJobServerModel testJob = assertJobCreation(jobType);
    jobDao.queued(testJob.getJobId());
    GenericJobServerModel actualJob = jobDao.getJob(testJob.getJobId());
    assertEquals(StatusEnum.QUEUED, actualJob.getStatus());
  }

  @Test
  void running() {
    JobType jobType = JobType.DATA_IMPORT;
    GenericJobServerModel testJob = assertJobCreation(jobType);
    jobDao.running(testJob.getJobId());
    GenericJobServerModel actualJob = jobDao.getJob(testJob.getJobId());
    assertEquals(StatusEnum.RUNNING, actualJob.getStatus());
  }

  @Test
  void success() {
    JobType jobType = JobType.DATA_IMPORT;
    GenericJobServerModel testJob = assertJobCreation(jobType);
    jobDao.succeeded(testJob.getJobId());
    GenericJobServerModel actualJob = jobDao.getJob(testJob.getJobId());
    assertEquals(StatusEnum.SUCCEEDED, actualJob.getStatus());
  }

  // TODO: AJ-1011 get job, does it deserialize correctly?
  @Test
  void getJob() {
    JobType jobType = JobType.DATA_IMPORT;
    GenericJobServerModel actual = assertJobCreation(jobType);

    assertEquals(JobTypeEnum.DATA_IMPORT, actual.getJobType());
    assertEquals(StatusEnum.CREATED, actual.getStatus());
    // TODO: AJ-1011 as PostgresJobDao.mapRow evolves, add more assertions here
  }
}
