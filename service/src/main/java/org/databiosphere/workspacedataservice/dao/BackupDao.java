package org.databiosphere.workspacedataservice.dao;

import org.databiosphere.workspacedataservice.service.BackupService;
import org.databiosphere.workspacedataservice.service.model.exception.MissingObjectException;
import org.databiosphere.workspacedataservice.shared.model.AsyncJob;
import org.databiosphere.workspacedataservice.shared.model.AsyncJobBuilder;
import org.databiosphere.workspacedataservice.shared.model.AsyncJobStatus;
import org.databiosphere.workspacedataservice.shared.model.BackupResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.stereotype.Component;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;

@Component
public class BackupDao {
    private static final Logger LOGGER = LoggerFactory.getLogger(BackupService.class);

    //TODO: in the future this will shift to "twds.instance.source-workspace-id"
    @Value("${twds.instance.workspace-id:}")
    private String workspaceId;

    @Value("${twds.pg_dump.user:}")
    private String dbUser;

    @Value("${twds.pg_dump.dbName:}")
    private String dbName;

    @Value("${twds.pg_dump.password:}")
    private String dbPassword;

    @Value("${twds.pg_dump.port:}")
    private String dbPort;

    @Value("${twds.pg_dump.host:}")
    private String dbHost;

    @Value("${twds.pg_dump.path:}")
    private String pgDumpPath;

    @Autowired
    private NamedParameterJdbcTemplate namedTemplate;

    public AsyncJob describeBackupJob(String jobId) {
        MapSqlParameterSource params = new MapSqlParameterSource("jobId", jobId);
        List<AsyncJob> jobs = namedTemplate.query(
                "select * from sys_wds.backup where id = :jobId",
                params, new AsyncJobRowMapper());

        if (jobs.isEmpty()) {
            throw new MissingObjectException("Background job");
        }

        if (jobs.size() > 1) {
            throw new RuntimeException("Unexpected result (" + jobs.size() + ") when retrieving job");
        }

        return jobs.get(0);
    }

    // rowmapper for retrieving SampleJobResponse objects from the db
    private static class AsyncJobRowMapper implements RowMapper<AsyncJob> {
        @Override
        public AsyncJob mapRow(ResultSet rs, int rowNum) throws SQLException {
            AsyncJobStatus status;
            try {
                status = AsyncJobStatus.valueOf(rs.getString("status"));
            } catch (IllegalArgumentException e) {
                status = AsyncJobStatus.UNKNOWN;
                LOGGER.warn(e.getMessage(), e);
            }

            BackupResult backupResult = null;
            if (rs.getString("backupResult") != null) {
                backupResult = new BackupResult(rs.getString("backupResult"));
            }

            return new AsyncJobBuilder(rs.getString("id"))
                .withStatus(status)
                .withCreated(rs.getTimestamp("created").toLocalDateTime())
                .withUpdated(rs.getTimestamp("updated").toLocalDateTime())
                    .withErrorMessage(rs.getString("errorMessage"))
                    // TODO: change "exception" to "stackTrace" and store as string
//                    .withException(rs.getString("exception"))
                    .withResult(backupResult)
                    .build();
        }
    }


}
