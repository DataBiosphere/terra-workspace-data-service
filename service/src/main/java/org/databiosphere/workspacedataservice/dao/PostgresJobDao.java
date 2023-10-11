package org.databiosphere.workspacedataservice.dao;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.ZoneOffset;
import java.util.UUID;
import org.databiosphere.workspacedataservice.generated.GenericJobServerModel;
import org.databiosphere.workspacedataservice.shared.model.job.Job;
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
  public GenericJobServerModel createJob(Job<?, ?> job) {
    // save the input arguments as a jsonb packet, being resilient to nulls
    String inputJsonb = null;
    if (job.getInput() != null) {
      try {
        inputJsonb = mapper.writeValueAsString(job.getInput());
      } catch (JsonProcessingException e) {
        // for now, fail silently. If/when we have any inputs that are required,
        // this should rethrow an exception instead of swallowing it
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
            "insert into sys_wds.job(id, type, status, input) " + "values (?, ?, ?, ?::jsonb)",
            job.getJobId().toString(),
            job.getJobType().name(),
            GenericJobServerModel.StatusEnum.CREATED.name(),
            inputJsonb);

    return getJob(job.getJobId());
  }

  @Override
  public GenericJobServerModel updateStatus(UUID jobId, GenericJobServerModel.StatusEnum status) {
    return this.updateStatus(jobId, status, null, null);
  }

  @Override
  public GenericJobServerModel updateStatus(
      UUID jobId, GenericJobServerModel.StatusEnum status, String errorMessage) {
    return this.updateStatus(jobId, status, errorMessage, null);
  }

  @Override
  public GenericJobServerModel updateStatus(
      UUID jobId,
      GenericJobServerModel.StatusEnum status,
      String errorMessage,
      StackTraceElement[] stackTrace) {
    // update this import job with a new status
    // note that the table's trigger will automatically update the `updated` column's value
    if (errorMessage == null && stackTrace == null) {
      namedTemplate
          .getJdbcTemplate()
          .update(
              "update sys_wds.job set status = ? where id = ?", status.name(), jobId.toString());
    } else if (stackTrace == null) {
      namedTemplate
          .getJdbcTemplate()
          .update(
              "update sys_wds.job set status = ?, error = ? where id = ?",
              status.name(),
              errorMessage,
              jobId.toString());
    } else {
      namedTemplate
          .getJdbcTemplate()
          .update(
              "update sys_wds.job set status = ?, error = ?, stacktrace = ? where id = ?",
              status.name(),
              errorMessage,
              stackTrace,
              jobId.toString());
    }
    return getJob(jobId);
  }

  @Override
  public GenericJobServerModel getJob(UUID jobId) {
    return namedTemplate.queryForObject(
        "select id, type, status, created, updated, "
            + "input, result, error, stacktrace "
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

      GenericJobServerModel.JobTypeEnum jobType = GenericJobServerModel.JobTypeEnum.UNKNOWN;
      String jobTypeStr = rs.getString("type");
      try {
        jobType = GenericJobServerModel.JobTypeEnum.fromValue(jobTypeStr);
      } catch (IllegalArgumentException ill) {
        logger.warn("Unexpected JobTypeEnum found: {}", jobTypeStr);
      }

      GenericJobServerModel.StatusEnum status = GenericJobServerModel.StatusEnum.UNKNOWN;
      String statusStr = rs.getString("status");
      try {
        status = GenericJobServerModel.StatusEnum.fromValue(statusStr);
      } catch (IllegalArgumentException ill) {
        logger.warn("Unexpected StatusEnum found: {}", statusStr);
      }

      var created = rs.getTimestamp("created").toLocalDateTime().atOffset(ZoneOffset.UTC);
      var updated = rs.getTimestamp("updated").toLocalDateTime().atOffset(ZoneOffset.UTC);

      GenericJobServerModel job =
          new GenericJobServerModel(jobId, jobType, status, created, updated);

      job.errorMessage(rs.getString("error"));

      // TODO:
      // - stacktrace
      // - input
      // - result
      return job;
    }
  }
}
