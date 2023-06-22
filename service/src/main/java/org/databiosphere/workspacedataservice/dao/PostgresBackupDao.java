package org.databiosphere.workspacedataservice.dao;

import bio.terra.common.db.WriteTransaction;
import org.databiosphere.workspacedataservice.service.model.BackupSchema;
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
import java.util.UUID;

@Repository
public class PostgresBackupDao implements BackupDao {

    @Value("${spring.datasource.username}")
    private String wdsDbUser;

    private static final Logger LOGGER = LoggerFactory.getLogger(PostgresBackupDao.class);

    private final NamedParameterJdbcTemplate namedTemplate;

    public PostgresBackupDao(NamedParameterJdbcTemplate namedTemplate) {
        this.namedTemplate = namedTemplate;
    }

    @Override
    public BackupSchema getBackupStatus(UUID trackingId) {
        try {
            MapSqlParameterSource params = new MapSqlParameterSource("trackingId", trackingId);
            return namedTemplate.query(
                    "select status from sys_wds.backup WHERE trackingId = :trackingId", params, new BackupSchemaRowMapper()).get(0);
        }
        catch(Exception e) {
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
    public boolean backupExistsForGivenSource(UUID sourceWorkspaceId) {
        return Boolean.TRUE.equals(namedTemplate.queryForObject(
                "select exists(select from sys_wds.backup WHERE sourceworkspaceid = :sourceWorkspaceId)",
                new MapSqlParameterSource("sourceWorkspaceId", sourceWorkspaceId), Boolean.class));
    }

    @Override
    @WriteTransaction
    @SuppressWarnings("squid:S2077") // since trackingId must be a UUID, it is safe to use inline
    public void createBackupEntry(UUID trackingId, UUID sourceWorkspaceId) {
        // an empty source id would basically mean that the backup record corresponds to current workspace id
        BackupSchema schema = new BackupSchema(trackingId, sourceWorkspaceId);
        UUID id = schema.getId();
        String status = String.valueOf(schema.getState());
        Timestamp createdTime = schema.getCreatedtime();
        Timestamp completedTime = schema.getCompletedtime();
        String error = schema.getError();
        UUID sourceId = schema.getSourceworkspaceid();
        namedTemplate.getJdbcTemplate().update("insert into sys_wds.backup(id, status, createdtime, completedtime, error, sourceworkspaceid) values (?,?,?,?,?,?)", id, status, createdTime, completedTime, error, schema.getSourceworkspaceid());
    }

    @Override
    @WriteTransaction
    @SuppressWarnings("squid:S2077") // since trackingId must be a UUID, it is safe to use inline
    public void updateBackupStatus(UUID trackingId, String status) {
        namedTemplate.getJdbcTemplate().update("update sys_wds.backup SET status = ? where id = ?", status, trackingId);
    }


    @Override
    @WriteTransaction
    @SuppressWarnings("squid:S2077") // since trackingId must be a UUID, it is safe to use inline
    public void updateFilename(UUID trackingId, String filename) {
        namedTemplate.getJdbcTemplate().update("update sys_wds.backup SET filename = ? where id = ?", filename, trackingId);
    }

    // rowmapper for retrieving BackupSchema objects from the db
    private static class BackupSchemaRowMapper implements RowMapper<BackupSchema> {
        @Override
        public BackupSchema mapRow(ResultSet rs, int rowNum) throws SQLException {
            BackupSchema backup = new BackupSchema();
            backup.setError(rs.getString("error"));
            backup.setState(BackupSchema.BackupState.valueOf(rs.getString("state")));
            backup.setFileName(rs.getString("fileName"));
            backup.setId(UUID.fromString(rs.getString("id")));
            backup.setSourceworkspaceid(UUID.fromString(rs.getString("upsourceworkspaceiddatedat")));
            return backup;
        }
    }
}
