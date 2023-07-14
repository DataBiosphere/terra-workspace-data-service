package org.databiosphere.workspacedataservice.dao;

import bio.terra.common.db.WriteTransaction;
import org.apache.commons.lang3.StringUtils;
import org.databiosphere.workspacedataservice.shared.model.BackupRestoreRequest;
import org.databiosphere.workspacedataservice.shared.model.job.Job;
import org.databiosphere.workspacedataservice.shared.model.job.JobResult;
import org.databiosphere.workspacedataservice.shared.model.job.JobStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;

import java.sql.Timestamp;
import java.time.Instant;
import java.util.UUID;

public class AbstractBackupRestoreDao <T extends JobResult> implements BackupRestoreDao<T> {

    protected final NamedParameterJdbcTemplate namedTemplate;

    protected final String dbName;

    /*
    AbstractBackupRestoreDao abstracts functionality used to interact with sys_wds backup and restore tables to track progress of each.
    Functionality for backup and restore specific content (such as overwriting dbName) will be implemented in PostgresBackupDao and PostgresRestoreDao.
    */
    public AbstractBackupRestoreDao(NamedParameterJdbcTemplate namedTemplate, String dbName) {
        this.namedTemplate = namedTemplate;
        this.dbName = dbName;
    }

    protected static final Logger LOGGER = LoggerFactory.getLogger(PostgresBackupDao.class);


    public Job<T> getStatus(UUID trackingId) {
        return null;
    }

    public void createEntry(UUID trackingId, BackupRestoreRequest BackupRestoreRequest) {}

    @Override
    @WriteTransaction
    public void updateStatus(UUID trackingId, JobStatus status) {
        namedTemplate.getJdbcTemplate().update(String.format("update sys_wds.%s SET status = ?, updatedtime = ? where id = ?", dbName),
                status.name(), Timestamp.from(Instant.now()), trackingId);
        LOGGER.info("Job {} is now {}", trackingId, status);
    }

    @Override
    @WriteTransaction
    public void terminateToError(UUID trackingId, String error) {

        namedTemplate.getJdbcTemplate().update(String.format("update sys_wds.%s SET error = ? where id = ?", dbName),
                StringUtils.abbreviate(error, 2000), trackingId);
        // because saveBackupError is annotated with @WriteTransaction, we can ignore IntelliJ warnings about
        // self-invocation of transactions on the following line:
        updateStatus(trackingId, JobStatus.ERROR);
    }

    @Override
    @WriteTransaction
    public void updateFilename(UUID trackingId, String filename) {
        namedTemplate.getJdbcTemplate().update(String.format("update sys_wds.%s SET filename = ? where id = ?", dbName), filename, trackingId);
    }
}