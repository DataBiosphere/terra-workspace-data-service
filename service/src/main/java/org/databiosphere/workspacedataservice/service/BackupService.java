package org.databiosphere.workspacedataservice.service;

import com.azure.identity.extensions.jdbc.postgresql.AzurePostgresqlAuthenticationPlugin;
import org.apache.commons.lang3.StringUtils;
import org.databiosphere.workspacedataservice.dao.BackupDao;
import org.databiosphere.workspacedataservice.process.LocalProcessLauncher;
import org.databiosphere.workspacedataservice.service.model.BackupSchema;
import org.databiosphere.workspacedataservice.service.model.exception.LaunchProcessException;
import org.databiosphere.workspacedataservice.shared.model.BackupResponse;
import org.databiosphere.workspacedataservice.storage.BackUpFileStorage;
import org.databiosphere.workspacedataservice.workspacemanager.WorkspaceManagerDao;
import org.postgresql.plugin.AuthenticationRequestType;
import org.postgresql.util.PSQLException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;

import static org.databiosphere.workspacedataservice.service.RecordUtils.validateVersion;

@Service
public class BackupService {
    private final BackupDao backupDao;
    private final WorkspaceManagerDao workspaceManagerDao;

    private BackUpFileStorage storage;
    private static final Logger LOGGER = LoggerFactory.getLogger(BackupService.class);

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
  
    @Value("${twds.pg_dump.useAzureIdentity:}")
    private boolean useAzureIdentity;


    public BackupService(BackupDao backupDao, WorkspaceManagerDao workspaceManagerDao, BackUpFileStorage backUpFileStorage) {
        this.backupDao = backupDao;
        this.workspaceManagerDao = workspaceManagerDao;
        this.storage = backUpFileStorage;
    }

    public BackupResponse checkBackupStatus(UUID trackingId) {
        var backup = backupDao.getBackupStatus(trackingId);

        if(backup !=null) {
            if (backup.getState() == BackupSchema.BackupState.COMPLETED) {
                return new BackupResponse(true, BackupSchema.BackupState.COMPLETED.toString(), backup.getFilename(), "Backup successfully completed.");
            } else if (backup.getState() == BackupSchema.BackupState.ERROR) {
                return new BackupResponse(true, BackupSchema.BackupState.ERROR.toString(), "", "Backup completed with an error.");
            } else {
                return new BackupResponse(false, backup.getState().toString(), "", "Backup still in progress.");
            }
        }
        else {
            backupDao.updateFilename(UUID.randomUUID(), "hi");
            return new BackupResponse(false, "", "", "Backup not found.");
        }
    }

    public void backupAzureWDS(String version, UUID trackingId, UUID requestorWorkspaceId) {
        try {
            validateVersion(version);

            // create an entry to track progress of this backup
            backupDao.createBackupEntry(trackingId);

            // create an entry to track who requested this backup
            backupDao.createBackupRequestsEntry(requestorWorkspaceId, UUID.fromString(workspaceId));

            String blobName = GenerateBackupFilename();

            List<String> commandList = GenerateCommandList();
            Map<String, String> envVars = Map.of("PGPASSWORD", determinePassword());

            LocalProcessLauncher localProcessLauncher = new LocalProcessLauncher();
            localProcessLauncher.launchProcess(commandList, envVars);

            backupDao.updateBackupStatus(trackingId, BackupSchema.BackupState.STARTED.toString());
            LOGGER.info("Starting streaming backup to storage.");
            storage.streamOutputToBlobStorage(localProcessLauncher.getInputStream(), blobName, workspaceId);
            String error = localProcessLauncher.getOutputForProcess(LocalProcessLauncher.Output.ERROR);
            int exitCode = localProcessLauncher.waitForTerminate();

            if (exitCode != 0 && StringUtils.isNotBlank(error)) {
                LOGGER.error("process error: {}", error);
                backupDao.updateBackupStatus(trackingId, BackupSchema.BackupState.ERROR.toString());
                backupDao.updateBackupRequestStatus(UUID.fromString(workspaceId), BackupSchema.BackupState.ERROR);
            }

            // if no errors happen and code reaches here, the backup has been completed successfully
            backupDao.updateFilename(trackingId, blobName);
            backupDao.updateBackupStatus(trackingId, BackupSchema.BackupState.COMPLETED.toString());
            backupDao.updateBackupRequestStatus(UUID.fromString(workspaceId), BackupSchema.BackupState.COMPLETED);
        }
        catch (LaunchProcessException | PSQLException ex) {
            LOGGER.error("process error: {}", ex);
            backupDao.updateBackupStatus(trackingId, BackupSchema.BackupState.ERROR.toString());
            backupDao.updateBackupRequestStatus(UUID.fromString(workspaceId), BackupSchema.BackupState.ERROR);
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

    public List<String> GenerateCommandList() {
        Map<String, String> command = new LinkedHashMap<>();
        command.put(pgDumpPath, null);
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

    public String GenerateBackupFilename() {
        LocalDateTime now = LocalDateTime.now();
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd_HH-mm-ss");
        String timestamp = now.format(formatter);
        return "wdsservice/cloning/backup/" + workspaceId + "-" + timestamp + ".sql";
    }
}

