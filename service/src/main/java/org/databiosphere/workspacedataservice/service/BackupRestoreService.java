package org.databiosphere.workspacedataservice.service;

import com.azure.identity.extensions.jdbc.postgresql.AzurePostgresqlAuthenticationPlugin;
import org.apache.commons.lang3.StringUtils;
import org.databiosphere.workspacedataservice.activitylog.ActivityLogger;
import org.databiosphere.workspacedataservice.dao.BackupRestoreDao;
import org.databiosphere.workspacedataservice.dao.CloneDao;
import org.databiosphere.workspacedataservice.dao.InstanceDao;
import org.databiosphere.workspacedataservice.process.LocalProcessLauncher;
import org.databiosphere.workspacedataservice.service.model.exception.LaunchProcessException;
import org.databiosphere.workspacedataservice.service.model.exception.MissingObjectException;
import org.databiosphere.workspacedataservice.shared.model.BackupRestoreRequest;
import org.databiosphere.workspacedataservice.shared.model.BackupResponse;
import org.databiosphere.workspacedataservice.shared.model.CloneResponse;
import org.databiosphere.workspacedataservice.shared.model.RestoreResponse;
import org.databiosphere.workspacedataservice.shared.model.job.Job;
import org.databiosphere.workspacedataservice.shared.model.job.JobStatus;
import org.databiosphere.workspacedataservice.storage.BackUpFileStorage;
import org.postgresql.plugin.AuthenticationRequestType;
import org.postgresql.util.PSQLException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.dao.DataAccessException;
import org.springframework.stereotype.Service;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;

import static org.databiosphere.workspacedataservice.service.RecordUtils.validateVersion;

@Service
public class BackupRestoreService {
    private final BackupRestoreDao<BackupResponse> backupDao;
    private final BackupRestoreDao<RestoreResponse> restoreDao;
    private final CloneDao cloneDao;
    private final BackUpFileStorage storage;
    private final InstanceDao instanceDao;
    private final ActivityLogger activityLogger;
    private static final Logger LOGGER = LoggerFactory.getLogger(BackupRestoreService.class);

    @Value("${twds.instance.workspace-id:}")
    private String workspaceId;

    @Value("${twds.instance.source-workspace-id:}")
    private String sourceWorkspaceId;

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

    @Value("${twds.pg_dump.psqlPath:}")
    private String psqlPath;

    @Value("${twds.pg_dump.useAzureIdentity:}")
    private boolean useAzureIdentity;

    public BackupRestoreService(BackupRestoreDao<BackupResponse> backupDao, 
                                BackupRestoreDao<RestoreResponse> restoreDao, 
                                InstanceDao instanceDao,
                                BackUpFileStorage backUpFileStorage,
                                CloneDao cloneDao,
                                ActivityLogger activityLogger) {
        this.backupDao = backupDao;
        this.restoreDao = restoreDao;
        this.instanceDao = instanceDao;
        this.cloneDao = cloneDao;
        this.storage = backUpFileStorage;
        this.activityLogger = activityLogger;
    }

    public Job<BackupResponse> checkBackupStatus(UUID trackingId) {
        var backupJob = backupDao.getStatus(trackingId);

        if (backupJob == null) {
            throw new MissingObjectException("Backup job");
        }

        return backupJob;
    }

    public Job<CloneResponse> checkCloneStatus() {
        return cloneDao.getCloneStatus();
    }

    public Job<BackupResponse> backupAzureWDS(String version, UUID trackingId, BackupRestoreRequest backupRestoreRequest) {
        try {
            validateVersion(version);

            // if request did not specify which workspace asked for the backup, default to the current workspace
            UUID requesterWorkspaceId = backupRestoreRequest.requestingWorkspaceId() == null ? UUID.fromString(workspaceId) : backupRestoreRequest.requestingWorkspaceId();

            // create an entry to track progress of this backup
            backupDao.createEntry(trackingId, backupRestoreRequest);

            String blobName = generateBackupFilename();

            List<String> commandList = generateCommandList(true);
            Map<String, String> envVars = Map.of("PGPASSWORD", determinePassword());

            LocalProcessLauncher localProcessLauncher = new LocalProcessLauncher();
            localProcessLauncher.launchProcess(commandList, envVars);

            backupDao.updateStatus(trackingId, JobStatus.RUNNING);
            LOGGER.info("Starting streaming backup to storage.");
            storage.streamOutputToBlobStorage(localProcessLauncher.getInputStream(), blobName, String.valueOf(requesterWorkspaceId));
            String error = checkForError(localProcessLauncher);

            if (StringUtils.isNotBlank(error)) {
                LOGGER.error("process error: {}", error);
                backupDao.terminateToError(trackingId, error);
            }
            else {
                // if no errors happen and code reaches here, the backup has been completed successfully
                backupDao.updateFilename(trackingId, blobName);
                backupDao.updateStatus(trackingId, JobStatus.SUCCEEDED);
                activityLogger.saveEventForCurrentUser(user ->
                        user.created().backup().withId(blobName));
            }
        }
        catch (Exception ex) {
            LOGGER.error("Process error: {}", ex.getMessage());
            backupDao.terminateToError(trackingId, ex.getMessage());
        }

        return backupDao.getStatus(trackingId);
    }

    public Job<RestoreResponse> restoreAzureWDS(String version, String backupFileName, UUID trackingId, String startupToken) {
        validateVersion(version);
        try {
            LOGGER.info("Starting restore. ");
            // create an entry to track progress of this restore
            restoreDao.createEntry(trackingId, new BackupRestoreRequest(UUID.fromString(workspaceId), String.format("Restore from %s", backupFileName)));

            // generate psql query
            List<String> commandList = generateCommandList(false);
            Map<String, String> envVars = Map.of("PGPASSWORD", determinePassword());

            restoreDao.updateStatus(trackingId, JobStatus.RUNNING);
            LocalProcessLauncher localProcessLauncher = new LocalProcessLauncher();
            localProcessLauncher.launchProcess(commandList, envVars);

            LOGGER.info("Grabbing data from the backup file. ");
            // grab blob from storage
            storage.streamInputFromBlobStorage(localProcessLauncher.getOutputStream(), backupFileName, workspaceId, startupToken);

            String error = checkForError(localProcessLauncher);
            if (StringUtils.isNotBlank(error)) {
                LOGGER.error("process error: {}", error);
                restoreDao.terminateToError(trackingId, error);
                return restoreDao.getStatus(trackingId);
            }

            // rename workspace schema from source to dest
            instanceDao.alterSchema(UUID.fromString(sourceWorkspaceId), UUID.fromString(workspaceId));
            // clean up
            storage.DeleteBlob(backupFileName, workspaceId);
            activityLogger.saveEventForCurrentUser(user ->
                user.restored().backup().withId(backupFileName));
            restoreDao.updateStatus(trackingId, JobStatus.SUCCEEDED);
        }
        catch (LaunchProcessException | PSQLException | DataAccessException ex){
            LOGGER.error("process error: {}", ex.getMessage());
            restoreDao.terminateToError(trackingId, ex.getMessage());
        }
        return restoreDao.getStatus(trackingId);
    }

    private String determinePassword() throws PSQLException {
        if (useAzureIdentity) {
            return new String(new AzurePostgresqlAuthenticationPlugin(new Properties())
                    .getPassword(AuthenticationRequestType.CLEARTEXT_PASSWORD));
        } else {
            return dbPassword;
        }
    }

    // Same args, different command depending on whether we're doing backup or restore.
    public List<String> generateCommandList(boolean isBackup) {
        Map<String, String> command = new LinkedHashMap<>();
        if (isBackup) {
            command.put(pgDumpPath, null);
            command.put("-b", null);
            // Grab all workspace instances/schemas in wds
            for (UUID id : instanceDao.listInstanceSchemas()) {
                command.put("-n", id.toString());
            }
        } else {
            command.put(psqlPath, null);
        }
        command.put("-h", dbHost);
        command.put("-p", dbPort);
        command.put("-U", dbUser);
        command.put("-d", dbName);

        List<String> commandList = new ArrayList<>();
        for (Map.Entry<String, String> entry : command.entrySet()) {
            commandList.add(entry.getKey());
            if (entry.getValue() != null) {
                commandList.add(entry.getValue());
            }
        }
        commandList.add("-v");
        commandList.add("-w");

        return commandList;
    }

    public String generateBackupFilename() {
        LocalDateTime now = LocalDateTime.now();
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd_HH-mm-ss");
        String timestamp = now.format(formatter);
        return "wdsservice/cloning/backup/" + workspaceId + "-" + timestamp + ".sql";
    }

    private String checkForError(LocalProcessLauncher localProcessLauncher) {
        String error = localProcessLauncher.getOutputForProcess(LocalProcessLauncher.Output.ERROR);
        int exitCode = localProcessLauncher.waitForTerminate();
        if (exitCode != 0 && StringUtils.isNotBlank(error)) {
            LOGGER.error("process error: {}", error);
            return error;
        }
        return "";
    }
}