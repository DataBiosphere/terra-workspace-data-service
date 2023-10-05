package org.databiosphere.workspacedataservice.dao;

import bio.terra.common.db.ReadTransaction;
import bio.terra.common.db.WriteTransaction;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.ZoneOffset;
import java.util.UUID;
import org.databiosphere.workspacedataservice.dataimport.ImportStatusResponse;
import org.databiosphere.workspacedataservice.generated.ImportRequestServerModel;
import org.databiosphere.workspacedataservice.generated.JobStatusServerModel;
import org.databiosphere.workspacedataservice.shared.model.job.JobStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.stereotype.Repository;

/** Read/write jobs via the sys_wds.job Postgres table */
@Repository
public class PostgresJobDao implements JobDao {

  private final Logger logger = LoggerFactory.getLogger(this.getClass());
  private final NamedParameterJdbcTemplate namedTemplate;
  private final ObjectMapper mapper;

  public PostgresJobDao(NamedParameterJdbcTemplate namedTemplate, ObjectMapper mapper) {
    this.namedTemplate = namedTemplate;
    this.mapper = mapper;
  }

  @Override
  @WriteTransaction
  public void createJob(String jobId, ImportRequestServerModel importJob) {

    // save the import options KVPs as a jsonb packet, being resilient to nulls
    String optionsJsonb = null;
    if (importJob.getOptions() != null) {
      try {
        optionsJsonb = mapper.writeValueAsString(importJob.getOptions());
      } catch (JsonProcessingException e) {
        // for now, fail silently. If/when we have any import options that are required,
        // this should rethrow an exception instead of swallowing it
        logger.error(
            "Error serializing options to jsonb for import job {}: {}", jobId, e.getMessage());
      }
    }

    // insert the import request to the db. Note that the created and updated
    // columns in the db are automatically handled by Postgres.
    namedTemplate
        .getJdbcTemplate()
        .update(
            "insert into sys_wds.job(id, type, status, input) " + "values (?, ?, ?, ?::jsonb)",
            jobId,
            importJob.getType().getValue(),
            JobStatus.CREATED.name(),
            optionsJsonb);
  }

  @Override
  @WriteTransaction
  public void updateStatus(String jobId, JobStatus status) {
    // update this import job with a new status
    // note that the table's trigger will automatically update the `updated` column's value
    namedTemplate
        .getJdbcTemplate()
        .update("update sys_wds.job set status = ? where id = ?", status.name(), jobId);
  }

  @Override
  @ReadTransaction
  public ImportStatusResponse getJob(UUID jobId) {
    return namedTemplate.queryForObject(
        "select id, type, status, created, updated, "
            + "input, result, error, stacktrace "
            + "from sys_wds.job "
            + "where id = :jobId",
        new MapSqlParameterSource("jobId", jobId.toString()),
        new AsyncJobRowMapper());
  }

  // rowmapper for retrieving SampleJobResponse objects from the db
  private static class AsyncJobRowMapper implements RowMapper<ImportStatusResponse> {
    @Override
    public ImportStatusResponse mapRow(ResultSet rs, int rowNum) throws SQLException {
      ImportStatusResponse job = new ImportStatusResponse();
      job.setJobId(UUID.fromString(rs.getString("id")));
      job.setStatus(JobStatusServerModel.StatusEnum.fromValue(rs.getString("status")));
      job.setCreated(rs.getTimestamp("created").toLocalDateTime().atOffset(ZoneOffset.UTC));
      job.setUpdated(rs.getTimestamp("updated").toLocalDateTime().atOffset(ZoneOffset.UTC));
      // TODO: include missing fields - errorMessage, url, result, etc.
      return job;
    }
  }
}
