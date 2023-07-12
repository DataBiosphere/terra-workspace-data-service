package org.databiosphere.workspacedataservice.service;

import com.azure.identity.extensions.jdbc.postgresql.AzurePostgresqlAuthenticationPlugin;
import org.apache.commons.lang3.StringUtils;
import org.databiosphere.workspacedataservice.dao.BackupDao;
import org.databiosphere.workspacedataservice.dao.InstanceDao;
import org.databiosphere.workspacedataservice.process.LocalProcessLauncher;
import org.databiosphere.workspacedataservice.service.model.exception.LaunchProcessException;
import org.databiosphere.workspacedataservice.service.model.exception.MissingObjectException;
import org.databiosphere.workspacedataservice.shared.model.BackupRequest;
import org.databiosphere.workspacedataservice.shared.model.BackupResponse;
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
    private final BackupDao backupDao;
    private final BackUpFileStorage storage;
    private final InstanceDao instanceDao;
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

    public BackupRestoreService(BackupDao backupDao, InstanceDao instanceDao, BackUpFileStorage backUpFileStorage) {
        this.backupDao = backupDao;
        this.instanceDao = instanceDao;
        this.storage = backUpFileStorage;
    }

    public Job<BackupResponse> checkBackupStatus(UUID trackingId) {
        var backupJob = backupDao.getBackupStatus(trackingId);

        if (backupJob == null) {
            throw new MissingObjectException("Backup job");
        }

        return backupJob;
    }

    public Job<BackupResponse> backupAzureWDS(String version, UUID trackingId, BackupRequest backupRequest) {
        try {
            validateVersion(version);

            // if request did not specify which workspace asked for the backup, default to the current workspace
            UUID requesterWorkspaceId = backupRequest.requestingWorkspaceId() == null ? UUID.fromString(workspaceId) : backupRequest.requestingWorkspaceId();

            // create an entry to track progress of this backup
            backupDao.createBackupEntry(trackingId, backupRequest);

            String blobName = generateBackupFilename();

            List<String> commandList = generateCommandList(true);
            Map<String, String> envVars = Map.of("PGPASSWORD", determinePassword());

            LocalProcessLauncher localProcessLauncher = new LocalProcessLauncher();
            localProcessLauncher.launchProcess(commandList, envVars);

            backupDao.updateBackupStatus(trackingId, JobStatus.RUNNING);
            LOGGER.info("Starting streaming backup to storage.");
            storage.streamOutputToBlobStorage(localProcessLauncher.getInputStream(), blobName, String.valueOf(requesterWorkspaceId));
            String error = checkForError(localProcessLauncher);

            if (StringUtils.isNotBlank(error)) {
                LOGGER.error("process error: {}", error);
                backupDao.terminateBackupToError(trackingId, error);
            }
            else {
                // if no errors happen and code reaches here, the backup has been completed successfully
                backupDao.updateFilename(trackingId, blobName);
                backupDao.updateBackupStatus(trackingId, JobStatus.SUCCEEDED);
            }
        }
        catch (Exception ex) {
            LOGGER.error("Process error: {}", ex.getMessage());
            backupDao.terminateBackupToError(trackingId, ex.getMessage());
        }

        return backupDao.getBackupStatus(trackingId);
    }

    public Job<BackupResponse> restoreAzureWDS(String version, String backupFileName, String startupToken) {
        validateVersion(version);
        try {
            // restore pgdump
            LOGGER.info("Starting restore. ");
            List<String> commandList = generateCommandList(false);
            Map<String, String> envVars = Map.of("PGPASSWORD", determinePassword());

            LocalProcessLauncher localProcessLauncher = new LocalProcessLauncher();
            localProcessLauncher.launchProcess(commandList, envVars);

            LOGGER.info("Grabbing data from the backup file. ");
            // grab blob from storage
            storage.streamInputFromBlobStorage(localProcessLauncher.getOutputStream(), backupFileName, workspaceId, startupToken);

            String error = checkForError(localProcessLauncher);
            if (StringUtils.isNotBlank(error)) {
                LOGGER.error("process error: {}", error);
                return new Job<BackupResponse>(new UUID(0, 0), JobStatus.ERROR, error, null, null, null);
                //throw new LaunchProcessException(error);
            }

            // rename workspace schema from source to dest
            instanceDao.alterSchema(UUID.fromString(sourceWorkspaceId), UUID.fromString(workspaceId));
            storage.DeleteBlob(backupFileName, workspaceId);
            return new Job<BackupResponse>(new UUID(0, 0), JobStatus.SUCCEEDED, "backup complete", null, null, null);
        }
        catch (LaunchProcessException | PSQLException | DataAccessException ex){
            LOGGER.error("process error: {}", ex.getMessage());
            return new Job<BackupResponse>(new UUID(0, 0), JobStatus.ERROR, ex.getMessage(), null, null, null);
            //throw new LaunchProcessException(ex.getMessage());
        }
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
        command.put(isBackup ? pgDumpPath : psqlPath, null);
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