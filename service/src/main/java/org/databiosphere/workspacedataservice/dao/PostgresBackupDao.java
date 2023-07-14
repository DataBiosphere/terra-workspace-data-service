package org.databiosphere.workspacedataservice.dao;

import bio.terra.common.db.WriteTransaction;
import org.databiosphere.workspacedataservice.shared.model.BackupRestoreRequest;
import org.databiosphere.workspacedataservice.shared.model.CloneTable;
import org.databiosphere.workspacedataservice.shared.model.BackupResponse;
import org.databiosphere.workspacedataservice.shared.model.job.Job;
import org.databiosphere.workspacedataservice.shared.model.job.JobStatus;
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
public class PostgresBackupDao extends AbstractBackupRestoreDao<BackupResponse> {

    /*
    PostgresBackupDao is used to interact with sys_wds backup table in postgres that tracks status of backups.
    This class will help add entries to the table, check if entries already exist and update them as necessary.
     */
    public PostgresBackupDao(NamedParameterJdbcTemplate namedTemplate) {
        super(namedTemplate, CloneTable.BACKUP);
    }

    @Override
    public Job<BackupResponse> getStatus(UUID trackingId) {
        MapSqlParameterSource params = new MapSqlParameterSource("trackingId", trackingId);
        List<Job<BackupResponse>> responses = namedTemplate.query(
                "select id, status, error, createdtime, updatedtime, requester, filename, description from sys_wds.backup WHERE id = :trackingId",
                params, new BackupJobRowMapper());
        if (responses.size() == 1) {
            return responses.get(0);
        } else if (responses.isEmpty()) {
            return null;
        } else {
            throw new RuntimeException("Unexpected error: %s rows found for backup status query".formatted(responses.size()));
        }
    }

    @Override
    @WriteTransaction
    public void createEntry(UUID trackingId, BackupRestoreRequest BackupRestoreRequest) {
        Timestamp now = Timestamp.from(Instant.now());
        namedTemplate.getJdbcTemplate().update("insert into sys_wds.backup(id, status, createdtime, updatedtime, requester, description) " +
                "values (?,?,?,?,?,?)", trackingId, JobStatus.QUEUED.name(), now, now, BackupRestoreRequest.requestingWorkspaceId(), BackupRestoreRequest.description());
        LOGGER.info("Backup job {} is now {}", trackingId, JobStatus.QUEUED);
    }

    // rowmapper for retrieving Job<BackupResponse> objects from the db
    private static class BackupJobRowMapper implements RowMapper<Job<BackupResponse>> {
        @Override
        public Job<BackupResponse> mapRow(ResultSet rs, int rowNum) throws SQLException {
            String filename = rs.getString("fileName");
            String description = rs.getString("description");
            UUID requester = rs.getObject("requester", UUID.class);
            BackupResponse backupResponse = new BackupResponse(filename, requester, description);

            UUID jobId = rs.getObject("id", UUID.class);
            JobStatus status = JobStatus.UNKNOWN;
            String dbStatus = rs.getString("status");
            try {
                status = JobStatus.valueOf(dbStatus);
            } catch (Exception e) {
                LOGGER.warn("Unknown status for backup job {}: [{}] with error {}", jobId, dbStatus, e.getMessage());
            }
            String errorMessage = rs.getString("error");
            LocalDateTime created = rs.getTimestamp("createdtime").toLocalDateTime();
            LocalDateTime updated = rs.getTimestamp("updatedtime").toLocalDateTime();

            return new Job<>(jobId, status, errorMessage, created, updated, backupResponse);
        }
    }
}