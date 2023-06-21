package org.databiosphere.workspacedataservice.service;

import org.apache.commons.lang3.StringUtils;
import org.databiosphere.workspacedataservice.process.LocalProcessLauncher;
import org.databiosphere.workspacedataservice.service.model.exception.LaunchProcessException;
import org.databiosphere.workspacedataservice.shared.model.BackupRestoreResponse;
import org.databiosphere.workspacedataservice.storage.BackUpFileStorage;
import org.postgresql.plugin.AuthenticationRequestType;
import org.postgresql.util.PSQLException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;

import static org.databiosphere.workspacedataservice.service.RecordUtils.validateVersion;

import com.azure.identity.extensions.jdbc.postgresql.AzurePostgresqlAuthenticationPlugin;

@Service
public class BackupRestoreService {
    private static final Logger LOGGER = LoggerFactory.getLogger(BackupRestoreService.class);

    private static final String BackupFileName = "backup.sql";

    //TODO: in the future this will shift to "twds.instance.source-workspace-id"
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

    public BackupRestoreResponse backupAzureWDS(BackUpFileStorage storage, String version) {
        try {
            validateVersion(version);
            String blobName = generateBackupFilename();

            List<String> commandList = generateCommandList(true);
            Map<String, String> envVars = Map.of("PGPASSWORD", determinePassword());

            LocalProcessLauncher localProcessLauncher = new LocalProcessLauncher();
            localProcessLauncher.launchProcess(commandList, envVars);

            storage.streamOutputToBlobStorage(localProcessLauncher.getInputStream(), blobName);
            String error = checkForError(localProcessLauncher, LOGGER);
            if (StringUtils.isNotBlank(error)) {
                return new BackupRestoreResponse(false, error);
            }
        }
        catch (LaunchProcessException | PSQLException ex){
            return new BackupRestoreResponse(false, ex.getMessage());
        }

        return new BackupRestoreResponse(true, "Backup successfully completed.");
    }

    public BackupRestoreResponse restoreAzureWDS(BackUpFileStorage storage, String version) {
        validateVersion(version);
        try {
            // grab blob from storage
            storage.downloadFromBlobStorage(BackupFileName);
            
            // restore pgdump
            List<String> commandList = generateCommandList(false);
            commandList.add("-f" + BackupFileName);
            Map<String, String> envVars = Map.of("PGPASSWORD", dbPassword);

            LocalProcessLauncher localProcessLauncher = new LocalProcessLauncher();
            localProcessLauncher.launchProcess(commandList, envVars);
            String error = checkForError(localProcessLauncher, LOGGER);
            if (StringUtils.isNotBlank(error)) {
                return new BackupRestoreResponse(false, error);
            }

            // rename workspace schema from source to dest
            commandList.remove(commandList.size() - 1);
            commandList.add(String.format("-c ALTER SCHEMA \"%s\" RENAME TO \"%s\"", sourceWorkspaceId, workspaceId));
            localProcessLauncher.launchProcess(commandList, envVars);
            error = checkForError(localProcessLauncher, LOGGER);
            if (StringUtils.isNotBlank(error)) {
                return new BackupRestoreResponse(false, error);
            }
            
            // delete backup file
            Files.deleteIfExists(Paths.get(BackupFileName));
            return new BackupRestoreResponse(true, "Successfully completed restore");
        } 
        catch (LaunchProcessException ex){
            return new BackupRestoreResponse(false, ex.getMessage());
        }
        catch(IOException ex) {
            return new BackupRestoreResponse(false, "Trouble deleting Azure Blob file, specifically:" + ex.getMessage());
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
        return workspaceId + "-" + timestamp + ".sql";
    }

    private String checkForError(LocalProcessLauncher localProcessLauncher, Logger log) {
        String error = localProcessLauncher.getOutputForProcess(LocalProcessLauncher.Output.ERROR);
        int exitCode = localProcessLauncher.waitForTerminate();
        if (exitCode != 0 && StringUtils.isNotBlank(error)) {
            log.error("process error: {}", error);
        }
        return error;
    }
}
