package org.databiosphere.workspacedataservice.dao;

import bio.terra.common.db.WriteTransaction;
import org.databiosphere.workspacedataservice.service.model.BackupSchema;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.stereotype.Repository;

import java.sql.Timestamp;
import java.util.UUID;

@Repository
public class PostgresBackupDao implements BackupDao {

    @Value("${spring.datasource.username}")
    private String wdsDbUser;

    private final NamedParameterJdbcTemplate namedTemplate;

    public PostgresBackupDao(NamedParameterJdbcTemplate namedTemplate) {
        this.namedTemplate = namedTemplate;
    }

    @Override
    public String getBackupStatus(UUID trackingId) {
        try {
            return namedTemplate.getJdbcTemplate().queryForObject(
                    "select status from sys_wds.backup WHERE trackingId = ?", String.class, trackingId);
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
}
