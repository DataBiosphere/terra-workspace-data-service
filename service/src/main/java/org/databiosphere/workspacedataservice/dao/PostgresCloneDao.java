package org.databiosphere.workspacedataservice.dao;

import bio.terra.common.db.WriteTransaction;
import org.apache.commons.lang3.StringUtils;
import org.databiosphere.workspacedataservice.shared.model.BackupResponse;
import org.databiosphere.workspacedataservice.shared.model.CloneResponse;
import org.databiosphere.workspacedataservice.shared.model.CloneStatus;
import org.databiosphere.workspacedataservice.shared.model.job.Job;
import org.databiosphere.workspacedataservice.shared.model.job.JobStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.stereotype.Repository;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Instant;
import java.time.LocalDateTime;
import java.util.List;
import java.util.UUID;

@Repository
public class PostgresCloneDao implements CloneDao {
    @Value("${twds.instance.workspace-id:}")
    private String workspaceId;
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
    public boolean cloneExistsForWorkspace(UUID sourceWorkspaceId)  {
        return Boolean.TRUE.equals(namedTemplate.queryForObject(
                "select exists(select from sys_wds.clone WHERE sourceworkspaceid = :sourceWorkspaceId)",
                new MapSqlParameterSource("sourceWorkspaceId", sourceWorkspaceId), Boolean.class));
    }

    @Override
    @WriteTransaction
    public void createCloneEntry(UUID trackingId, UUID sourceWorkspaceId) {
        Timestamp now = Timestamp.from(Instant.now());
        namedTemplate.getJdbcTemplate().update("insert into sys_wds.clone(id, status, createdtime, updatedtime, sourceworkspaceid, clonestatus) " +
                "values (?,?,?,?,?,?)", trackingId, JobStatus.QUEUED.name(), now, now, sourceWorkspaceId, CloneStatus.BACKUPQUEUED.name());
    }

    @Override
    @WriteTransaction
    public void updateCloneEntryStatus(UUID trackingId, CloneStatus status) {
        try {
            var jobStatus = status.equals(CloneStatus.BACKUPSUCCEEDED) ? JobStatus.SUCCEEDED.name() : JobStatus.ERROR.name();
            namedTemplate.getJdbcTemplate().update("update sys_wds.clone SET clonestatus = ?, status = ?, updatedtime = ? where id = ?",
                    status.name(), jobStatus, Timestamp.from(Instant.now()), trackingId);
            LOGGER.info("Clone status is now {}.", status);
        }
        catch (Exception e){
            terminateCloneToError(trackingId, e.getMessage());
        }
    }

    @Override
    @WriteTransaction
    public void terminateCloneToError(UUID trackingId, String error) {
        namedTemplate.getJdbcTemplate().update("update sys_wds.clone SET error = ? where id = ?",
                StringUtils.abbreviate(error, 2000), trackingId);
        updateCloneEntryStatus(trackingId, CloneStatus.BACKUPERROR);
    }

    @Override
    public Job<CloneResponse> getCloneStatus() {
        List<Job<CloneResponse>> responses = namedTemplate.query(
                "select id, status, error, createdtime, updatedtime, sourceworkspaceid, clonestatus from sys_wds.clone",
                 new PostgresCloneDao.CloneJobRowMapper());
        if (responses.size() == 1) {
            return responses.get(0);
        } else if (responses.isEmpty()) {
            return null;
        } else {
            throw new RuntimeException("Unexpected error: %s rows found for backup status query".formatted(responses.size()));
        }
    }

    // rowmapper for retrieving Job<CloneResponse> objects from the db
    private static class CloneJobRowMapper implements RowMapper<Job<CloneResponse>> {
        @Override
        public Job<CloneResponse> mapRow(ResultSet rs, int rowNum) throws SQLException {
            UUID sourceworksapceid = rs.getObject("sourceworkspaceid", UUID.class);
            CloneStatus cloneStatus = CloneStatus.UNKNOWN;
            String cloneStatusString = rs.getString("clonestatus");

            UUID jobId = rs.getObject("id", UUID.class);
            JobStatus status = JobStatus.UNKNOWN;
            String dbStatus = rs.getString("status");
            try {
                status = JobStatus.valueOf(dbStatus);
            } catch (Exception e) {
                LOGGER.warn("Unknown status for clone job {}: [{}] with error {}", jobId, dbStatus, e.getMessage());
            }

            try {
                cloneStatus = CloneStatus.valueOf(cloneStatusString);
            } catch (Exception e) {
                LOGGER.warn("Unknown clone status for clone job {}: [{}] with error {}", jobId, cloneStatus, e.getMessage());
            }

            CloneResponse cloneResponse = new CloneResponse(sourceworksapceid, cloneStatus);

            String errorMessage = rs.getString("error");
            LocalDateTime created = rs.getTimestamp("createdtime").toLocalDateTime();
            LocalDateTime updated = rs.getTimestamp("updatedtime").toLocalDateTime();

            return new Job<>(jobId, status, errorMessage, created, updated, cloneResponse);
        }
    }
}
