package org.databiosphere.workspacedataservice.dao;

import bio.terra.common.db.WriteTransaction;
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
    public boolean cloneExistsForWorkspace(UUID requester)  {
        return Boolean.TRUE.equals(namedTemplate.queryForObject(
                "select exists(select from sys_wds.clone WHERE requester = :requester)",
                new MapSqlParameterSource("requester", requester), Boolean.class));
    }

    @Override
    @WriteTransaction
    public void createCloneEntry(UUID trackingId, UUID sourceWorkspaceId) {
        Timestamp now = Timestamp.from(Instant.now());
        namedTemplate.getJdbcTemplate().update("insert into sys_wds.clone(id, status, createdtime, updatedtime, sourceworkspaceid, cloneStatus) " +
                "values (?,?,?,?,?,?)", trackingId, JobStatus.QUEUED.name(), now, now, sourceWorkspaceId, CloneStatus.BACKUPQUEUED.name());
    }

    @Override
    @WriteTransaction
    public void updateCloneEntryStatus(UUID sourceWorkspaceId, CloneStatus status) {
        namedTemplate.getJdbcTemplate().update("update sys_wds.clone SET cloneStatus = ?, status = ? where sourceWorkspaceId = ?",
                status.name(), JobStatus.SUCCEEDED, sourceWorkspaceId);
        LOGGER.info("Clone status for workspace {} is now {}", sourceWorkspaceId, status);
    }
}
