package org.databiosphere.workspacedataservice.service;

import com.azure.identity.extensions.jdbc.postgresql.AzurePostgresqlAuthenticationPlugin;
import org.apache.commons.lang3.StringUtils;
import org.databiosphere.workspacedataservice.dao.BackupDao;
import org.databiosphere.workspacedataservice.dao.InstanceDao;
import org.databiosphere.workspacedataservice.process.LocalProcessLauncher;
import org.databiosphere.workspacedataservice.service.model.BackupSchema;
import org.databiosphere.workspacedataservice.service.model.exception.LaunchProcessException;
import org.databiosphere.workspacedataservice.shared.model.BackupRequest;
import org.databiosphere.workspacedataservice.shared.model.BackupResponse;
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
    private BackUpFileStorage storage;
    private final InstanceDao instanceDao;
    private static final Logger LOGGER = LoggerFactory.getLogger(BackupRestoreService.class);
    private static final String BackupFileName = "backup.sql";

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

    public BackupResponse checkBackupStatus(UUID trackingId) {
        var backup = backupDao.getBackupStatus(trackingId);

        if(backup != null) {
            if (backup.getState() == BackupSchema.BackupState.COMPLETED) {
                return new BackupResponse(true, BackupSchema.BackupState.COMPLETED.toString(), backup.getFilename(), "Backup successfully completed.");
            } else if (backup.getState() == BackupSchema.BackupState.ERROR) {
                return new BackupResponse(true, BackupSchema.BackupState.ERROR.toString(), "", "Backup completed with an error.");
            } else {
                return new BackupResponse(false, backup.getState().toString(), "", "Backup still in progress.");
            }
        }
        else {
            return new BackupResponse(false, "", "", "Backup not found.");
        }
    }

    public void backupAzureWDS(String version, UUID trackingId, BackupRequest backupRequest) {
        try {
            validateVersion(version);

            UUID requestorWorkspaceId = backupRequest.requestingWorkspaceId() == null ? UUID.fromString(workspaceId) : backupRequest.requestingWorkspaceId();

            // create an entry to track progress of this backup
            backupDao.createBackupEntry(trackingId);

            // create an entry to track who requested this backup
            backupDao.createBackupRequestsEntry(requestorWorkspaceId, UUID.fromString(workspaceId));
            String blobName = generateBackupFilename();

            List<String> commandList = generateCommandList(true);
            Map<String, String> envVars = Map.of("PGPASSWORD", determinePassword());

            LocalProcessLauncher localProcessLauncher = new LocalProcessLauncher();
            localProcessLauncher.launchProcess(commandList, envVars);

            backupDao.updateBackupStatus(trackingId, BackupSchema.BackupState.STARTED);
            LOGGER.info("Starting streaming backup to storage.");
            storage.streamOutputToBlobStorage(localProcessLauncher.getInputStream(), blobName, String.valueOf(requestorWorkspaceId));
            String error = checkForError(localProcessLauncher);

            if (StringUtils.isNotBlank(error)) {
                LOGGER.error("process error: {}", error);
                backupDao.updateBackupStatus(trackingId, BackupSchema.BackupState.ERROR);
                backupDao.updateBackupRequestStatus(UUID.fromString(workspaceId), BackupSchema.BackupState.ERROR);
            }
            else {
                // if no errors happen and code reaches here, the backup has been completed successfully
                backupDao.updateFilename(trackingId, blobName);
                backupDao.updateBackupStatus(trackingId, BackupSchema.BackupState.COMPLETED);
                backupDao.updateBackupRequestStatus(UUID.fromString(workspaceId), BackupSchema.BackupState.COMPLETED);
            }
        }
        catch (LaunchProcessException | PSQLException ex) {
            LOGGER.error("Process error: {}", ex.getMessage());
            backupDao.updateBackupStatus(trackingId, BackupSchema.BackupState.ERROR);
            backupDao.updateBackupRequestStatus(UUID.fromString(workspaceId), BackupSchema.BackupState.ERROR);
        }
    }

    public boolean restoreAzureWDS(String version) {
        validateVersion(version);
        try {
            // restore pgdump
            List<String> commandList = generateCommandList(false);
            Map<String, String> envVars = Map.of("PGPASSWORD", determinePassword());

            LocalProcessLauncher localProcessLauncher = new LocalProcessLauncher();
            localProcessLauncher.launchProcess(commandList, envVars);

            // grab blob from storage
            storage.streamInputFromBlobStorage(localProcessLauncher.getOutputStream(), BackupFileName, workspaceId);

            String error = checkForError(localProcessLauncher);
            if (StringUtils.isNotBlank(error)) {
                return false;
                //throw new LaunchProcessException(error);
            }

            // rename workspace schema from source to dest
            instanceDao.alterSchema(UUID.fromString(sourceWorkspaceId), UUID.fromString(workspaceId));

            return true;
        }
        catch (LaunchProcessException | PSQLException | DataAccessException ex){
            return false;
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
