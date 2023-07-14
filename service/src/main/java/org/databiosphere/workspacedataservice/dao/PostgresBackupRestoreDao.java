package org.databiosphere.workspacedataservice.dao;

import bio.terra.common.db.WriteTransaction;
import org.apache.commons.lang3.StringUtils;
import org.databiosphere.workspacedataservice.shared.model.BackupRestoreRequest;
import org.databiosphere.workspacedataservice.shared.model.BackupRestoreResponse;
import org.databiosphere.workspacedataservice.shared.model.job.Job;
import org.databiosphere.workspacedataservice.shared.model.job.JobStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
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
public class PostgresBackupRestoreDao implements BackupRestoreDao {

    private final NamedParameterJdbcTemplate namedTemplate;

    /*
    PostgresBackupRestoreDao is used to interact with sys_wds backup table in postgres that tracks status of backups.
    This class will help add entries to the table, check if entries already exist and update them as necessary.
     */
    public PostgresBackupRestoreDao(NamedParameterJdbcTemplate namedTemplate) {
        this.namedTemplate = namedTemplate;
    }

    private static final Logger LOGGER = LoggerFactory.getLogger(PostgresBackupRestoreDao.class);

    @Override
    public Job<BackupRestoreResponse> getStatus(UUID trackingId, Boolean isBackup) {
        MapSqlParameterSource params = new MapSqlParameterSource("trackingId", trackingId);
        List<Job<BackupRestoreResponse>> responses = namedTemplate.query(
                String.format("select id, status, error, createdtime, updatedtime, requester, filename, description from sys_wds.%s WHERE id = :trackingId", isBackup ? "backup" : "restore"),
                params, new BackupJobRowMapper());
        if (responses.size() == 1) {
            return responses.get(0);
        } else if (responses.isEmpty()) {
            return null;
        } else {
            throw new RuntimeException("Unexpected error: %s rows found for %s status query".formatted(responses.size(), isBackup ? "backup" : "restore"));
        }
    }

    @Override
    @WriteTransaction
    public void createEntry(UUID trackingId, BackupRestoreRequest BackupRestoreRequest, Boolean isBackup) {
        Timestamp now = Timestamp.from(Instant.now());
        namedTemplate.getJdbcTemplate().update(String.format("insert into sys_wds.%s(id, status, createdtime, updatedtime, requester, description) ", isBackup ? "backup" : "restore") +
                "values (?,?,?,?,?,?)", trackingId, JobStatus.QUEUED.name(), now, now, BackupRestoreRequest.requestingWorkspaceId(), BackupRestoreRequest.description());
        LOGGER.info("{} job {} is now {}", isBackup ? "Backup" : "Restore", trackingId, JobStatus.QUEUED);
    }

    @Override
    @WriteTransaction
    public void updateStatus(UUID trackingId, JobStatus status, Boolean isBackup) {
        namedTemplate.getJdbcTemplate().update(String.format("update sys_wds.%s SET status = ?, updatedtime = ? where id = ?", isBackup ? "backup" : "restore"),
                status.name(), Timestamp.from(Instant.now()), trackingId);
        LOGGER.info("{} job {} is now {}", isBackup ? "Backup" : "Restore", trackingId, status);
    }

    @Override
    @WriteTransaction
    public void terminateToError(UUID trackingId, String error, Boolean isBackup) {

        namedTemplate.getJdbcTemplate().update(String.format("update sys_wds.%s SET error = ? where id = ?", isBackup ? "backup" : "restore"),
                StringUtils.abbreviate(error, 2000), trackingId);
        // because saveBackupError is annotated with @WriteTransaction, we can ignore IntelliJ warnings about
        // self-invocation of transactions on the following line:
        updateStatus(trackingId, JobStatus.ERROR, isBackup);
    }

    @Override
    @WriteTransaction
    public void updateFilename(UUID trackingId, String filename, Boolean isBackup) {
        namedTemplate.getJdbcTemplate().update(String.format("update sys_wds.%s SET filename = ? where id = ?", isBackup ? "backup" : "restore"), filename, trackingId);
    }

    // rowmapper for retrieving Job<BackupRestoreResponse> objects from the db
    private static class BackupJobRowMapper implements RowMapper<Job<BackupRestoreResponse>> {
        @Override
        public Job<BackupRestoreResponse> mapRow(ResultSet rs, int rowNum) throws SQLException {
            String filename = rs.getString("fileName");
            String description = rs.getString("description");
            UUID requester = rs.getObject("requester", UUID.class);
            BackupRestoreResponse BackupRestoreResponse = new BackupRestoreResponse(filename, requester, description);

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

            return new Job<>(jobId, status, errorMessage, created, updated, BackupRestoreResponse);
        }
    }
}