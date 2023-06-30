package org.databiosphere.workspacedataservice.dao;

import bio.terra.common.db.WriteTransaction;
import org.databiosphere.workspacedataservice.service.model.BackupSchema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.stereotype.Repository;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.UUID;

@Repository
public class PostgresBackupDao implements BackupDao {

    private final NamedParameterJdbcTemplate namedTemplate;

    public PostgresBackupDao(NamedParameterJdbcTemplate namedTemplate) {
        this.namedTemplate = namedTemplate;
    }

    private static final Logger LOGGER = LoggerFactory.getLogger(PostgresBackupDao.class);

    @Override
    public BackupSchema getBackupStatus(UUID trackingId) {
        try {
            MapSqlParameterSource params = new MapSqlParameterSource("trackingId", trackingId);
            return namedTemplate.query(
                    "select id, status, createdtime, completedtime, error, filename from sys_wds.backup WHERE id = :trackingId", params, new BackupSchemaRowMapper()).get(0);
        }
        catch(Exception e) {
            LOGGER.error("Unable to insert record into sys_wds.backup due to error {}.", e.getMessage());
            return null;
        }
    }

    @Override
    public String getBackupRequestStatus(UUID sourceWorkspaceId, UUID destinationWorkspaceId) {
        try {
            return namedTemplate.getJdbcTemplate().queryForObject(
                    "select status from sys_wds.backup_requests WHERE sourceworkspaceid = ? and destinationworkspaceid = ?", String.class, sourceWorkspaceId, destinationWorkspaceId);
        }
        catch(Exception e) {
            LOGGER.error("Unable to insert record into sys_wds.backup_requests due to error {}.", e.getMessage());
            return null;
        }
    }

    @Override
    public boolean backupExists(UUID trackingId) {
        return Boolean.TRUE.equals(namedTemplate.queryForObject(
                "select exists(select from sys_wds.backup WHERE id = :trackingId)",
                new MapSqlParameterSource("trackingId", trackingId), Boolean.class));
    }

    @Override
    @WriteTransaction
    public void createBackupEntry(UUID trackingId) {
        BackupSchema schema = new BackupSchema(trackingId);
        namedTemplate.getJdbcTemplate().update("insert into sys_wds.backup(id, status, createdtime, completedtime, error) " +
                "values (?,?,?,?,?)", schema.getId(), String.valueOf(schema.getState()), schema.getCreatedtime(), schema.getCompletedtime(), schema.getError());
    }

    @Override
    @WriteTransaction
    public void createBackupRequestsEntry(UUID sourceWorkspaceId, UUID destinationWorkspaceId) {
        namedTemplate.getJdbcTemplate().update("insert into sys_wds.backup_requests(sourceworkspaceid, destinationworkspaceid, status) " +
                "values (?,?,?)", sourceWorkspaceId, destinationWorkspaceId, BackupSchema.BackupState.INITIATED.toString());
    }

    @Override
    @WriteTransaction
    public void updateBackupStatus(UUID trackingId, BackupSchema.BackupState status) {
        // TODO need to also update completed time (if this is for completed or error backups)
        namedTemplate.getJdbcTemplate().update("update sys_wds.backup SET status = ? where id = ?", status.toString(), trackingId);
        LOGGER.info("Backup request job is now {}", status);
    }

    @Override
    @WriteTransaction
    public void updateBackupRequestStatus(UUID sourceWorkspaceId, BackupSchema.BackupState status) {
        // TODO need to also update completed time (if this is for completed or error backups)
        namedTemplate.getJdbcTemplate().update("update sys_wds.backup_requests SET status = ? where sourceworkspaceid = ?", status.toString(), sourceWorkspaceId);
        LOGGER.info("Backup request job is now {}", status);
    }

    @Override
    @WriteTransaction
    public void updateFilename(UUID trackingId, String filename) {
        namedTemplate.getJdbcTemplate().update("update sys_wds.backup SET filename = ? where id = ?", filename, trackingId);
    }

    // rowmapper for retrieving BackupSchema objects from the db
    private static class BackupSchemaRowMapper implements RowMapper<BackupSchema> {
        @Override
        public BackupSchema mapRow(ResultSet rs, int rowNum) throws SQLException {
            BackupSchema backup = new BackupSchema();
            backup.setError(rs.getString("error"));
            backup.setState(BackupSchema.BackupState.valueOf(rs.getString("status")));
            backup.setFileName(rs.getString("fileName"));
            backup.setId(UUID.fromString(rs.getString("id")));
            return backup;
        }
    }
}
