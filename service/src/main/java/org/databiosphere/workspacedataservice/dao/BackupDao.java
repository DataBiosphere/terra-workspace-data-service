package org.databiosphere.workspacedataservice.dao;

import bio.terra.common.db.WriteTransaction;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.stereotype.Repository;

@Repository
public class BackupDao {

    @Value("${spring.datasource.username}")
    private String wdsDbUser;

    private final NamedParameterJdbcTemplate namedTemplate;

    public BackupDao(NamedParameterJdbcTemplate namedTemplate) {
        this.namedTemplate = namedTemplate;
    }

    public String getBackupStatus() {
        return namedTemplate.getJdbcTemplate().queryForRowSet(
                "select id from sys_wds.instance order by id").getString("state");
    }

    @WriteTransaction
    @SuppressWarnings("squid:S2077") // since instanceId must be a UUID, it is safe to use inline
    public void createBackupEntry(String trackingId) {
        namedTemplate.getJdbcTemplate().update("insert into sys_wds.backup(id) values (?)", trackingId);
    }

    @WriteTransaction
    @SuppressWarnings("squid:S2077") // since instanceId must be a UUID, it is safe to use inline
    public void updateBackupStatus(String trackingId, String status) {
        namedTemplate.getJdbcTemplate().update("update sys_wds.backup SET status=? where id = ?", status, trackingId);
    }

}
