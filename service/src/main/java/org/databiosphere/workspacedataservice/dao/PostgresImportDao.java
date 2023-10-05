package org.databiosphere.workspacedataservice.dao;

import bio.terra.common.db.WriteTransaction;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.ZoneOffset;
import org.databiosphere.workspacedataservice.dataimport.ImportStatusResponse;
import org.databiosphere.workspacedataservice.generated.ImportJobStatusServerModel;
import org.databiosphere.workspacedataservice.generated.ImportRequestServerModel;
import org.databiosphere.workspacedataservice.shared.model.job.JobStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.stereotype.Repository;

/** Read/write data import requests via the sys_wds.import Postgres table */
@Repository
public class PostgresImportDao implements ImportDao {

  private final Logger logger = LoggerFactory.getLogger(this.getClass());
  private final NamedParameterJdbcTemplate namedTemplate;
  private final ObjectMapper mapper;

  public PostgresImportDao(NamedParameterJdbcTemplate namedTemplate, ObjectMapper mapper) {
    this.namedTemplate = namedTemplate;
    this.mapper = mapper;
  }

  @Override
  @WriteTransaction
  public void createImport(String jobId, ImportRequestServerModel importJob) {

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
            "insert into sys_wds.import(id, type, status, url, options) "
                + "values (?, ?, ?, ?, ?::jsonb)",
            jobId,
            importJob.getType().getValue(),
            JobStatus.CREATED.name(),
            importJob.getUrl().toString(),
            optionsJsonb);
  }

  @Override
  @WriteTransaction
  public void updateStatus(String jobId, JobStatus status) {
    // update this import job with a new status
    namedTemplate
        .getJdbcTemplate()
        .update("update sys_wds.import set status = ? where id = ?", status.name(), jobId);
  }

  @Override
  public ImportStatusResponse getImport(String jobId) {
    return namedTemplate.queryForObject(
        "select id, type, status, url, created, updated, "
            + "options, result, error, stacktrace "
            + "from sys_wds.import "
            + "where id = :jobId",
        new MapSqlParameterSource("jobId", jobId),
        new AsyncJobRowMapper());
  }

  // rowmapper for retrieving SampleJobResponse objects from the db
  private static class AsyncJobRowMapper implements RowMapper<ImportStatusResponse> {
    @Override
    public ImportStatusResponse mapRow(ResultSet rs, int rowNum) throws SQLException {
      ImportStatusResponse job = new ImportStatusResponse();
      job.setJobId(rs.getString("id"));
      job.setStatus(ImportJobStatusServerModel.StatusEnum.fromValue(rs.getString("status")));
      job.setCreated(rs.getTimestamp("created").toLocalDateTime().atOffset(ZoneOffset.UTC));
      job.setUpdated(rs.getTimestamp("updated").toLocalDateTime().atOffset(ZoneOffset.UTC));
      return job;
    }
  }
}
