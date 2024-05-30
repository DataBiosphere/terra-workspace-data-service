package org.databiosphere.workspacedataservice.dao;

import static org.databiosphere.workspacedataservice.shared.model.job.JobType.SYNC_CLONE;

import bio.terra.common.db.WriteTransaction;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Instant;
import java.time.LocalDateTime;
import java.util.List;
import java.util.UUID;
import org.apache.commons.lang3.StringUtils;
import org.databiosphere.workspacedataservice.shared.model.CloneResponse;
import org.databiosphere.workspacedataservice.shared.model.CloneStatus;
import org.databiosphere.workspacedataservice.shared.model.CloneTable;
import org.databiosphere.workspacedataservice.shared.model.WorkspaceId;
import org.databiosphere.workspacedataservice.shared.model.job.Job;
import org.databiosphere.workspacedataservice.shared.model.job.JobInput;
import org.databiosphere.workspacedataservice.shared.model.job.JobStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.stereotype.Repository;

@Repository
public class PostgresCloneDao implements CloneDao {
  private final NamedParameterJdbcTemplate namedTemplate;

  /*
  PostgresCloneDao is used to interact with sys_wds clone table in postgres that tracks the overall status of cloning operation between two workspaces.
  This class will help add entries to the table, check if entries already exist and update them as necessary.
   */
  public PostgresCloneDao(NamedParameterJdbcTemplate namedTemplate) {
    this.namedTemplate = namedTemplate;
  }

  private static final Logger LOGGER = LoggerFactory.getLogger(PostgresCloneDao.class);

  @Override
  public boolean cloneExistsForWorkspace(WorkspaceId sourceWorkspaceId) {
    return Boolean.TRUE.equals(
        namedTemplate.queryForObject(
            "select exists(select from sys_wds.clone WHERE sourceworkspaceid = :sourceWorkspaceId)",
            new MapSqlParameterSource("sourceWorkspaceId", sourceWorkspaceId.id()),
            Boolean.class));
  }

  @Override
  @WriteTransaction
  public void createCloneEntry(UUID trackingId, WorkspaceId sourceWorkspaceId) {
    Timestamp now = Timestamp.from(Instant.now());
    namedTemplate
        .getJdbcTemplate()
        .update(
            "insert into sys_wds.clone(id, status, createdtime, updatedtime, sourceworkspaceid, clonestatus) "
                + "values (?,?,?,?,?,?)",
            trackingId,
            JobStatus.QUEUED.name(),
            now,
            now,
            sourceWorkspaceId.id(),
            CloneStatus.BACKUPQUEUED.name());
  }

  @Override
  @WriteTransaction
  public void updateCloneEntryStatus(UUID trackingId, CloneStatus cloneStatus) {
    try {
      // determine the overall job status based on the "clonestatus" sub-status
      JobStatus jobStatus =
          switch (cloneStatus) {
            case RESTORESUCCEEDED:
              yield JobStatus.SUCCEEDED;
            case BACKUPERROR, RESTOREERROR, UNKNOWN:
              yield JobStatus.ERROR;
            default:
              yield JobStatus.RUNNING;
          };

      namedTemplate
          .getJdbcTemplate()
          .update(
              "update sys_wds.clone SET clonestatus = ?, status = ?, updatedtime = ? where id = ?",
              cloneStatus.name(),
              jobStatus.name(),
              Timestamp.from(Instant.now()),
              trackingId);
      LOGGER.info("Clone status is now {}.", cloneStatus);
    } catch (Exception e) {
      // because updateCloneEntryStatus is itself annotated with @WriteTransaction, we can ignore
      // IntelliJ warnings about
      // self-invocation of transactions on the following line:
      //noinspection SpringTransactionalMethodCallsInspection
      terminateCloneToError(
          trackingId,
          e.getMessage(),
          cloneStatus.name().contains("BACKUP") ? CloneTable.BACKUP : CloneTable.RESTORE);
    }
  }

  @Override
  @WriteTransaction
  public void terminateCloneToError(UUID trackingId, String error, CloneTable table) {
    namedTemplate
        .getJdbcTemplate()
        .update(
            "update sys_wds.clone SET error = ? where id = ?",
            StringUtils.abbreviate(error, 2000),
            trackingId);
    // because terminateCloneToError is itself annotated with @WriteTransaction, we can ignore
    // IntelliJ warnings about
    // self-invocation of transactions on the following line:
    //noinspection SpringTransactionalMethodCallsInspection
    updateCloneEntryStatus(
        trackingId,
        table.equals(CloneTable.BACKUP) ? CloneStatus.BACKUPERROR : CloneStatus.RESTOREERROR);
  }

  /*
    If a workspace starts up in clone mode, its overall state will be recorded in a single row, saving the source workspace id
    along with the status of the cloning operations. If no data is returned by this function (i.e. null) it is safe to assume
    that the following workspace was not created from a clone.
  */
  @Override
  public Job<JobInput, CloneResponse> getCloneStatus() {
    List<Job<JobInput, CloneResponse>> responses =
        namedTemplate.query(
            "select id, status, error, createdtime, updatedtime, sourceworkspaceid, clonestatus from sys_wds.clone",
            new PostgresCloneDao.CloneJobRowMapper());
    if (responses.size() == 1) {
      return responses.get(0);
    } else if (responses.isEmpty()) {
      return null;
    } else {
      throw new RuntimeException(
          "Unexpected error: %s rows found for backup status query".formatted(responses.size()));
    }
  }

  // rowmapper for retrieving Job<CloneResponse> objects from the db
  private static class CloneJobRowMapper implements RowMapper<Job<JobInput, CloneResponse>> {
    @Override
    public Job<JobInput, CloneResponse> mapRow(ResultSet rs, int rowNum) throws SQLException {
      UUID sourceWorkspaceId = rs.getObject("sourceworkspaceid", UUID.class);
      CloneStatus cloneStatus = CloneStatus.UNKNOWN;
      String cloneStatusString = rs.getString("clonestatus");

      UUID jobId = rs.getObject("id", UUID.class);
      JobStatus status = JobStatus.UNKNOWN;
      String dbStatus = rs.getString("status");
      try {
        status = JobStatus.valueOf(dbStatus);
      } catch (Exception e) {
        LOGGER.warn(
            "Unknown status for clone job {}: [{}] with error {}", jobId, dbStatus, e.getMessage());
      }

      try {
        cloneStatus = CloneStatus.valueOf(cloneStatusString);
      } catch (Exception e) {
        LOGGER.warn(
            "Unknown clone status for clone job {}: [{}] with error {}",
            jobId,
            cloneStatus,
            e.getMessage());
      }

      CloneResponse cloneResponse = new CloneResponse(sourceWorkspaceId, cloneStatus);

      String errorMessage = rs.getString("error");
      LocalDateTime created = rs.getTimestamp("createdtime").toLocalDateTime();
      LocalDateTime updated = rs.getTimestamp("updatedtime").toLocalDateTime();

      return new Job<>(
          jobId,
          SYNC_CLONE,
          /* instanceId= */ null, // clone jobs do not execute within a single instance
          status,
          errorMessage,
          created,
          updated,
          JobInput.empty(),
          cloneResponse);
    }
  }
}
