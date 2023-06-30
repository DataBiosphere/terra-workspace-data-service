package org.databiosphere.workspacedataservice.dao;

import bio.terra.common.db.WriteTransaction;
import org.apache.commons.lang3.StringUtils;
import org.databiosphere.workspacedataservice.service.model.BackupSchema;
import org.databiosphere.workspacedataservice.shared.model.BackupRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.stereotype.Repository;

import javax.annotation.Nullable;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Instant;
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
                    "select id, status, error, createdtime, updatedtime, requester, filename, description from sys_wds.backup WHERE id = :trackingId", params, new BackupSchemaRowMapper()).get(0);
        }
        catch(Exception e) {
            LOGGER.error("Unable to insert record into sys_wds.backup due to error {}.", e.getMessage());
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
    public void createBackupEntry(UUID trackingId, BackupRequest backupRequest) {
        BackupSchema schema = new BackupSchema(trackingId, backupRequest);
        namedTemplate.getJdbcTemplate().update("insert into sys_wds.backup(id, status, createdtime, updatedtime, requester, description) " +
                "values (?,?,?,?,?,?)", schema.getId(), String.valueOf(schema.getState()), schema.getCreatedtime(), schema.getUpdatedtime(), schema.getRequester(), schema.getDescription());
    }

    @Override
    @WriteTransaction
    public void updateBackupStatus(UUID trackingId, BackupSchema.BackupState status) {
        // TODO need to also update completed time (if this is for completed or error backups)
        namedTemplate.getJdbcTemplate().update("update sys_wds.backup SET status = ?, updatedtime = ? where id = ?",
                status.toString(), Timestamp.from(Instant.now()), trackingId);
        LOGGER.info("Backup request job is now {}", status);
    }

    @Override
    @WriteTransaction
    public void saveBackupError(UUID trackingId, String error) {

        namedTemplate.getJdbcTemplate().update("update sys_wds.backup SET error = ? where id = ?",
                StringUtils.abbreviate(error, 2000), trackingId);
        // because saveBackupError is annotated with @WriteTransaction, we can ignore IntelliJ warnings about
        // self-invocation of transactions on the following line:
        updateBackupStatus(trackingId, BackupSchema.BackupState.ERROR);
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
            backup.setId(UUID.fromString(rs.getString("id")));
            backup.setState(BackupSchema.BackupState.valueOf(rs.getString("status")));
            backup.setError(rs.getString("error"));
            backup.setCreatedtime(rs.getTimestamp("createdtime"));
            backup.setUpdatedtime(rs.getTimestamp("updatedtime"));
            backup.setRequester(UUID.fromString(rs.getString("requester")));
            backup.setFileName(rs.getString("fileName"));
            backup.setDescription(rs.getString("description"));
            return backup;
        }
    }
}