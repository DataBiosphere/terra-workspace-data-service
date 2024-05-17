package org.databiosphere.workspacedataservice.dao;

import static java.util.Objects.requireNonNull;
import static org.assertj.core.api.Assertions.assertThat;
import static org.databiosphere.workspacedataservice.generated.GenericJobServerModel.JobTypeEnum;
import static org.databiosphere.workspacedataservice.generated.GenericJobServerModel.StatusEnum;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Metrics;
import io.micrometer.core.instrument.Tag;
import io.micrometer.core.instrument.Tags;
import io.micrometer.core.instrument.Timer;
import java.net.URI;
import java.net.URISyntaxException;
import java.time.Duration;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import org.databiosphere.workspacedataservice.common.MockInstantSource;
import org.databiosphere.workspacedataservice.common.MockInstantSourceConfig;
import org.databiosphere.workspacedataservice.common.TestBase;
import org.databiosphere.workspacedataservice.dataimport.ImportJobInput;
import org.databiosphere.workspacedataservice.generated.GenericJobServerModel;
import org.databiosphere.workspacedataservice.generated.ImportRequestServerModel.TypeEnum;
import org.databiosphere.workspacedataservice.shared.model.CollectionId;
import org.databiosphere.workspacedataservice.shared.model.job.Job;
import org.databiosphere.workspacedataservice.shared.model.job.JobInput;
import org.databiosphere.workspacedataservice.shared.model.job.JobResult;
import org.databiosphere.workspacedataservice.shared.model.job.JobType;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Import;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.test.annotation.DirtiesContext;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@DirtiesContext
@SpringBootTest
@Import(MockInstantSourceConfig.class)
class PostgresJobDaoTest extends TestBase {
  private static final String TEST_IMPORT_URI = "http://some/uri";

  // createJob
  // updateStatus x 3
  // getJob

  @Autowired NamedParameterJdbcTemplate namedTemplate;
  @Autowired PostgresJobDao jobDao;
  @Autowired ObjectMapper mapper;
  @Autowired MeterRegistry metrics;
  @Autowired MockInstantSource mockInstantSource;

  @AfterEach
  void afterEach() {
    metrics.clear();
    Metrics.globalRegistry.clear();
    // cleanup: delete everything from the job table
    namedTemplate.getJdbcTemplate().update("delete from sys_wds.job;");
  }

  private GenericJobServerModel assertJobCreation(JobType jobType) {
    CollectionId collectionId = CollectionId.of(UUID.randomUUID());
    ImportJobInput jobInput = makeJobInput(TEST_IMPORT_URI, TypeEnum.PFB);
    Job<JobInput, JobResult> testJob = Job.newJob(collectionId, jobType, jobInput);

    jobDao.createJob(testJob);

    var params = new MapSqlParameterSource("jobId", testJob.getJobId().toString());
    params.addValue("type", jobType.name());
    params.addValue("collectionId", collectionId.id());
    params.addValue("status", StatusEnum.CREATED.name());
    try {
      params.addValue("input", mapper.writeValueAsString(jobInput));
    } catch (JsonProcessingException e) {
      throw new RuntimeException(e);
    }

    // after creating a job, there should be exactly one row with:
    // this jobId, the specified job type, and in status = CREATED,
    // created and updated timestamps being not null,
    // result and error and stacktrace are null
    // and input is {}
    assertDoesNotThrow(
        () ->
            namedTemplate.queryForObject(
                "select id from sys_wds.job where id = :jobId and type = :type and status = :status "
                    + "and collection_id = :collectionId "
                    + "and created is not null and updated is not null "
                    + "and input = :input::jsonb "
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
  void update(StatusEnum status) throws JsonProcessingException {
    JobType jobType = JobType.DATA_IMPORT;
    ImportJobInput jobInput = makeJobInput(TEST_IMPORT_URI, TypeEnum.PFB);
    GenericJobServerModel testJob = assertJobCreation(jobType);
    jobDao.updateStatus(testJob.getJobId(), status);

    // after updating the job, there should be exactly one row with:
    // this jobId and type, the new status,
    // and updated timestamp greater than the created timestamp.
    // input should still be {}, and result, error, and stacktrace should still be null
    var params = new MapSqlParameterSource("jobId", testJob.getJobId().toString());
    params.addValue("type", jobType.name());
    params.addValue("status", status.name());
    params.addValue("input", mapper.writeValueAsString(jobInput));
    assertDoesNotThrow(
        () ->
            namedTemplate.queryForObject(
                "select id from sys_wds.job where id = :jobId and type = :type and status = :status "
                    + "and updated > created "
                    + "and input = :input::jsonb "
                    + "and result is null and error is null and stacktrace is null",
                params,
                String.class),
        "should properly update the job");
  }

  @Test
  void updateStatusMeasuresElapsedTime() {
    // Arrange
    Job<JobInput, JobResult> job =
        Job.newJob(
            CollectionId.of(UUID.randomUUID()),
            JobType.DATA_IMPORT,
            makeJobInput("http://some/uri", TypeEnum.PFB));

    UUID jobId = jobDao.createJob(job).getJobId();
    mockInstantSource.add(Duration.ofMinutes(6));

    // Act
    jobDao.updateStatus(jobId, StatusEnum.SUCCEEDED);

    // Assert
    Tags expectedTags =
        Tags.of(
            Tag.of("jobType", "DATA_IMPORT"),
            Tag.of("importType", "PFB"),
            Tag.of("newStatus", "SUCCEEDED"));

    Timer timer = requireNonNull(metrics.find("wds.job.elapsed").timer());
    assertThat(timer.count()).isEqualTo(1);
    assertThat(timer.totalTime(TimeUnit.MINUTES)).isGreaterThanOrEqualTo(5);
    assertThat(timer.getId().getTags()).containsAll(expectedTags);
  }

  // fail the job with an error message
  @Test
  void failWithErrorMessage() throws JsonProcessingException {
    JobType jobType = JobType.DATA_IMPORT;
    ImportJobInput jobInput = makeJobInput(TEST_IMPORT_URI, TypeEnum.PFB);
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
    params.addValue("input", mapper.writeValueAsString(jobInput));
    params.addValue("error", errorMessage);

    assertDoesNotThrow(
        () ->
            namedTemplate.queryForObject(
                "select id from sys_wds.job where id = :jobId and type = :type and status = :status "
                    + "and error = :error "
                    + "and updated > created "
                    + "and input = :input::jsonb "
                    + "and result is null and stacktrace is null",
                params,
                String.class),
        "should properly update the job with an error message");
  }

  // fail the job with an exception
  @Test
  void failWithException() throws JsonProcessingException {
    JobType jobType = JobType.DATA_IMPORT;
    ImportJobInput jobInput = makeJobInput(TEST_IMPORT_URI, TypeEnum.PFB);
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
    params.addValue("input", mapper.writeValueAsString(jobInput));
    params.addValue("error", errorMessage);
    params.addValue("stacktrace", mapper.writeValueAsString(stackTrace));
    assertDoesNotThrow(
        () ->
            namedTemplate.queryForObject(
                "select id from sys_wds.job where id = :jobId and type = :type and status = :status "
                    + "and error = :error "
                    + "and stacktrace = :stacktrace::jsonb "
                    + "and updated > created "
                    + "and input = :input::jsonb "
                    + "and result is null",
                params,
                String.class),
        "should properly update the job with an error message and a stack trace");
  }

  // fail the job with a custom error message and an exception
  @Test
  void failWithExceptionAndMessage() throws JsonProcessingException {
    JobType jobType = JobType.DATA_IMPORT;
    ImportJobInput jobInput = makeJobInput(TEST_IMPORT_URI, TypeEnum.PFB);
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
    params.addValue("input", mapper.writeValueAsString(jobInput));
    params.addValue("error", customErrorMessage);
    params.addValue("stacktrace", mapper.writeValueAsString(stackTrace));
    assertDoesNotThrow(
        () ->
            namedTemplate.queryForObject(
                "select id from sys_wds.job where id = :jobId and type = :type and status = :status "
                    + "and error = :error "
                    + "and stacktrace = :stacktrace::jsonb "
                    + "and updated > created "
                    + "and input = :input::jsonb "
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
    assertEquals(makeJobInput(TEST_IMPORT_URI, TypeEnum.PFB), actual.getInput());

    assertNotNull(actual.getInstanceId());
    // TODO: AJ-1011 as PostgresJobDao.mapRow evolves, add more assertions here
  }

  @Test
  void getOldRunningJobs() {
    // Set up some jobs
    JobType jobType = JobType.DATA_IMPORT;
    CollectionId collectionId = CollectionId.of(UUID.randomUUID());
    ImportJobInput jobInput = makeJobInput(TEST_IMPORT_URI, TypeEnum.PFB);
    Job<JobInput, JobResult> job1 = Job.newJob(collectionId, jobType, jobInput);

    // job1 - status CREATED
    jobDao.createJob(job1);

    // job2 - status RUNNING
    Job<JobInput, JobResult> job2 = Job.newJob(collectionId, jobType, jobInput);
    GenericJobServerModel runningJob = jobDao.createJob(job2);
    jobDao.running(runningJob.getJobId());

    // job3 - status SUCCEEDED
    Job<JobInput, JobResult> job3 = Job.newJob(collectionId, jobType, jobInput);
    GenericJobServerModel finishedJob = jobDao.createJob(job3);
    jobDao.succeeded(finishedJob.getJobId());

    List<GenericJobServerModel> noJobs = jobDao.getOldNonTerminalJobs();
    assertThat(noJobs.isEmpty());

    // Let time pass without the job statuses updating
    mockInstantSource.add(Duration.ofHours(7));

    // Should fetch the CREATED and RUNNING jobs
    List<GenericJobServerModel> jobs = jobDao.getOldNonTerminalJobs();
    assertEquals(2, jobs.size());
  }

  private static ImportJobInput makeJobInput(String testImportUri, TypeEnum importType) {
    try {
      return new ImportJobInput(new URI(testImportUri), importType);
    } catch (URISyntaxException e) {
      throw new RuntimeException(e);
    }
  }
}
