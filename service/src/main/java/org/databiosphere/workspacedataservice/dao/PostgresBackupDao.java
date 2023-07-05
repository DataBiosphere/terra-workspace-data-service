package org.databiosphere.workspacedataservice.dao;

import bio.terra.common.db.WriteTransaction;
import org.apache.commons.lang3.StringUtils;
import org.databiosphere.workspacedataservice.shared.model.BackupRequest;
import org.databiosphere.workspacedataservice.shared.model.BackupResponse;
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
public class PostgresBackupDao implements BackupDao {

    private final NamedParameterJdbcTemplate namedTemplate;

    /*
    PostgresBackupDao is used to interact with sys_wds backup table in postgres that tracks status of backups.
    This class will help add entries to the table, check if entries already exist and udpate them as necessary.
     */
    public PostgresBackupDao(NamedParameterJdbcTemplate namedTemplate) {
        this.namedTemplate = namedTemplate;
    }

    private static final Logger LOGGER = LoggerFactory.getLogger(PostgresBackupDao.class);

    @Override
    public Job<BackupResponse> getBackupStatus(UUID trackingId) {
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
    public boolean backupExists(UUID trackingId) {
        return Boolean.TRUE.equals(namedTemplate.queryForObject(
                "select exists(select from sys_wds.backup WHERE id = :trackingId)",
                new MapSqlParameterSource("trackingId", trackingId), Boolean.class));
    }

    @Override
    public boolean backupExistsForWorkspace(UUID requester)  {
        return Boolean.TRUE.equals(namedTemplate.queryForObject(
                "select exists(select from sys_wds.backup WHERE requester = :requester)",
                new MapSqlParameterSource("requester", requester), Boolean.class));
    }

    @Override
    @WriteTransaction
    public void createBackupEntry(UUID trackingId, BackupRequest backupRequest) {
        Timestamp now = Timestamp.from(Instant.now());
        namedTemplate.getJdbcTemplate().update("insert into sys_wds.backup(id, status, createdtime, updatedtime, requester, description) " +
                "values (?,?,?,?,?,?)", trackingId, JobStatus.QUEUED.name(), now, now, backupRequest.requestingWorkspaceId(), backupRequest.description());
        LOGGER.info("Backup job {} is now {}", trackingId, JobStatus.QUEUED);
    }

    @Override
    @WriteTransaction
    public void updateBackupStatus(UUID trackingId, JobStatus status) {
        namedTemplate.getJdbcTemplate().update("update sys_wds.backup SET status = ?, updatedtime = ? where id = ?",
                status.name(), Timestamp.from(Instant.now()), trackingId);
        LOGGER.info("Backup job {} is now {}", trackingId, status);
    }

    @Override
    @WriteTransaction
    public void terminateBackupToError(UUID trackingId, String error) {

        namedTemplate.getJdbcTemplate().update("update sys_wds.backup SET error = ? where id = ?",
                StringUtils.abbreviate(error, 2000), trackingId);
        // because saveBackupError is annotated with @WriteTransaction, we can ignore IntelliJ warnings about
        // self-invocation of transactions on the following line:
        updateBackupStatus(trackingId, JobStatus.ERROR);
    }

    @Override
    @WriteTransaction
    public void updateFilename(UUID trackingId, String filename) {
        namedTemplate.getJdbcTemplate().update("update sys_wds.backup SET filename = ? where id = ?", filename, trackingId);
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