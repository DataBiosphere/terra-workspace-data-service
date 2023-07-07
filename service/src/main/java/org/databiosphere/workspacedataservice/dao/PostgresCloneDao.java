package org.databiosphere.workspacedataservice.dao;

import bio.terra.common.db.WriteTransaction;
import org.apache.commons.lang3.StringUtils;
import org.databiosphere.workspacedataservice.shared.model.CloneStatus;
import org.databiosphere.workspacedataservice.shared.model.job.JobStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.stereotype.Repository;

import java.sql.Timestamp;
import java.time.Instant;
import java.util.UUID;

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
            namedTemplate.getJdbcTemplate().update("update sys_wds.clone SET clonestatus = ?, status = ? where id = ?",
                    status.name(), JobStatus.SUCCEEDED.name(), trackingId);
            LOGGER.info("Clone status is now {}", status);
        }
        catch (Exception e){
            namedTemplate.getJdbcTemplate().update("update sys_wds.clone SET status = ? where id = ?",
                    JobStatus.ERROR.name(), trackingId);
        }
    }

    @Override
    @WriteTransaction
    public void terminateBackupToError(UUID trackingId, String error) {
        namedTemplate.getJdbcTemplate().update("update sys_wds.clone SET error = ? where id = ?",
                StringUtils.abbreviate(error, 2000), trackingId);
        updateCloneEntryStatus(trackingId, CloneStatus.BACKUPERROR);
    }
}
