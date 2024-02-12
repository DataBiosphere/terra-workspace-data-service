package org.databiosphere.workspacedataservice.dao;

import static org.databiosphere.workspacedataservice.generated.GenericJobServerModel.JobTypeEnum;
import static org.databiosphere.workspacedataservice.generated.GenericJobServerModel.StatusEnum;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Preconditions;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.ZoneOffset;
import java.util.UUID;
import org.databiosphere.workspacedataservice.generated.GenericJobServerModel;
import org.databiosphere.workspacedataservice.shared.model.job.Job;
import org.databiosphere.workspacedataservice.shared.model.job.JobInput;
import org.databiosphere.workspacedataservice.shared.model.job.JobResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.stereotype.Repository;

/** Read/write jobs via the sys_wds.job Postgres table */
@Repository
public class PostgresJobDao implements JobDao {

  private static final Logger logger = LoggerFactory.getLogger(PostgresJobDao.class);
  private final NamedParameterJdbcTemplate namedTemplate;
  private final ObjectMapper mapper;

  public PostgresJobDao(NamedParameterJdbcTemplate namedTemplate, ObjectMapper mapper) {
    this.namedTemplate = namedTemplate;
    this.mapper = mapper;
  }

  @Override
  public GenericJobServerModel createJob(Job<JobInput, JobResult> job) {
    // save the input arguments as a jsonb packet, being resilient to nulls
    String inputJsonb = null;
    if (job.getInput() != null) {
      try {
        inputJsonb = mapper.writeValueAsString(job.getInput());
      } catch (JsonProcessingException e) {
        // for now, fail silently. If/when we have any inputs that are required,
        // this should rethrow an exception instead of swallowing it
        // TODO: AJ-1011 this will soon need to be a failure
        logger.error(
            "Error serializing inputs to jsonb for job {}: {}", job.getJobId(), e.getMessage());
      }
    }

    // insert the job request to the db. Note that the created and updated
    // columns in the db are automatically handled by Postgres.
    // ignore result, error, and stacktrace; these can't be set at job creation.
    namedTemplate
        .getJdbcTemplate()
        .update(
            "insert into sys_wds.job(id, type, instance_id, status, input) "
                + "values (?, ?, ?, ?, ?::jsonb)",
            job.getJobId().toString(),
            job.getJobType().name(),
            job.getInstanceId().id(),
            StatusEnum.CREATED.name(),
            inputJsonb);

    return getJob(job.getJobId());
  }

  /**
   * Mark a job as QUEUED.
   *
   * @param jobId the job to update
   * @return the updated job
   */
  @Override
  public GenericJobServerModel queued(UUID jobId) {
    return updateStatus(jobId, StatusEnum.QUEUED);
  }

  /**
   * Mark a job as RUNNING.
   *
   * @param jobId the job to update
   * @return the updated job
   */
  @Override
  public GenericJobServerModel running(UUID jobId) {
    return updateStatus(jobId, StatusEnum.RUNNING);
  }

  /**
   * Mark a job as SUCCEEDED.
   *
   * @param jobId the job to update
   * @return the updated job
   */
  @Override
  public GenericJobServerModel succeeded(UUID jobId) {
    return updateStatus(jobId, StatusEnum.SUCCEEDED);
  }

  /**
   * update this import job with a new status. note that the table's trigger will automatically
   * update the `updated` column's value. Do not use this method to mark a job as failed; use one of
   * the fail() methods instead.
   *
   * @param jobId id of the job to update
   * @param status the status to which the job will be updated
   * @return the updated job
   */
  @Override
  public GenericJobServerModel updateStatus(UUID jobId, StatusEnum status) {
    Preconditions.checkArgument(!StatusEnum.ERROR.equals(status), "Use fail() instead");
    Preconditions.checkArgument(
        !StatusEnum.UNKNOWN.equals(status), "Do not set status to UNKNOWN directly");
    return update(jobId, status, null, null);
  }

  /**
   * Mark a job as failed, specifying a short human-readable error message.
   *
   * @param jobId id of the job to update
   * @param errorMessage a short error message, if the job is in error
   * @return the updated job
   */
  @Override
  public GenericJobServerModel fail(UUID jobId, String errorMessage) {
    return update(jobId, StatusEnum.ERROR, errorMessage, null);
  }

  /**
   * Mark a job as failed, specifying the Exception that caused the failure
   *
   * @param jobId id of the job to update
   * @param e the exception that caused this job to fail
   * @return the updated job
   */
  @Override
  public GenericJobServerModel fail(UUID jobId, Exception e) {
    return fail(jobId, e.getMessage(), e);
  }

  /**
   * Mark a job as failed, specifying a short human-readable error message and the Exception that
   * caused the failure
   *
   * @param jobId id of the job to update
   * @param errorMessage a short error message, if the job is in error
   * @return the updated job
   */
  @Override
  public GenericJobServerModel fail(UUID jobId, String errorMessage, Exception e) {
    return update(jobId, StatusEnum.ERROR, errorMessage, e.getStackTrace());
  }

  private GenericJobServerModel update(
      UUID jobId, StatusEnum status, String errorMessage, StackTraceElement[] stackTrace) {

    // start our sql statement and map of params
    StringBuilder sb = new StringBuilder("update sys_wds.job set status = :status");
    MapSqlParameterSource params = new MapSqlParameterSource("jobId", jobId.toString());
    params.addValue("status", status.name());

    // if an error message is supplied, also update that
    if (errorMessage != null) {
      params.addValue("error", errorMessage);
      sb.append(", error = :error");
    }

    // if a stack trace is supplied, also update that, making sure to serialize to jsonb
    if (stackTrace != null) {
      try {
        String stackTraceJsonb = mapper.writeValueAsString(stackTrace);
        params.addValue("stacktrace", stackTraceJsonb);
        sb.append(", stacktrace = :stacktrace::jsonb");
      } catch (JsonProcessingException e) {
        logger.error(
            "Error serializing stack trace to jsonb for job {}: {}", jobId, e.getMessage());
        // TODO: AJ-1011 should this be fatal?
      }
    }

    // make sure to have a where clause!!
    sb.append(" where id = :jobId;");

    // execute the update
    namedTemplate.update(sb.toString(), params);

    logger.debug("Job {} is now in status {}", jobId, status);

    // return the updated job
    return getJob(jobId);
  }

  /**
   * Retrieve a job.
   *
   * @param jobId the job to retrieve
   * @return the retrieved job
   */
  @Override
  public GenericJobServerModel getJob(UUID jobId) {
    return namedTemplate.queryForObject(
        "select id, type, status, created, updated, "
            + "input, result, error, stacktrace, instance_id "
            + "from sys_wds.job "
            + "where id = :jobId",
        new MapSqlParameterSource("jobId", jobId.toString()),
        new AsyncJobRowMapper());
  }

  // rowmapper for retrieving Job objects from the db
  private static class AsyncJobRowMapper implements RowMapper<GenericJobServerModel> {
    @Override
    public GenericJobServerModel mapRow(ResultSet rs, int rowNum) throws SQLException {
      UUID jobId = UUID.fromString(rs.getString("id"));

      JobTypeEnum jobType = JobTypeEnum.UNKNOWN;
      String jobTypeStr = rs.getString("type");
      try {
        jobType = JobTypeEnum.fromValue(jobTypeStr);
      } catch (IllegalArgumentException ill) {
        logger.warn("Unexpected JobTypeEnum found: {}", jobTypeStr);
      }

      StatusEnum status = StatusEnum.UNKNOWN;
      String statusStr = rs.getString("status");
      try {
        status = StatusEnum.fromValue(statusStr);
      } catch (IllegalArgumentException ill) {
        logger.warn("Unexpected StatusEnum found: {}", statusStr);
      }

      var created = rs.getTimestamp("created").toLocalDateTime().atOffset(ZoneOffset.UTC);
      var updated = rs.getTimestamp("updated").toLocalDateTime().atOffset(ZoneOffset.UTC);

      UUID instanceId = rs.getObject("instance_id", UUID.class);
      GenericJobServerModel job =
          new GenericJobServerModel(jobId, jobType, instanceId, status, created, updated);

      job.errorMessage(rs.getString("error"));

      // TODO: AJ-1011 also return stacktrace, input, result.
      return job;
    }
  }
}
