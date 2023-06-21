package org.databiosphere.workspacedataservice.service;

import bio.terra.common.db.WriteTransaction;
import org.apache.commons.lang3.StringUtils;
import org.databiosphere.workspacedataservice.process.LocalProcessLauncher;
import org.databiosphere.workspacedataservice.service.model.exception.LaunchProcessException;
import org.databiosphere.workspacedataservice.shared.model.BackupRestoreResponse;
import org.databiosphere.workspacedataservice.storage.BackUpFileStorage;
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

@Service
public class BackupRestoreService {
    private static final Logger LOGGER = LoggerFactory.getLogger(BackupRestoreService.class);

    private final String backupName = "backup.sql";

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

    @Value("${twds.pg_dump.psqlPath:}")
    private String psqlPath;

    public BackupRestoreResponse backupAzureWDS(BackUpFileStorage storage, String version) {
        try {
            validateVersion(version);
            String blobName = GenerateBackupFilename();

            List<String> commandList = GenerateCommandList(true);
            Map<String, String> envVars = Map.of("PGPASSWORD", dbPassword);

            LocalProcessLauncher localProcessLauncher = new LocalProcessLauncher();
            localProcessLauncher.launchProcess(commandList, envVars);

            storage.streamOutputToBlobStorage(localProcessLauncher.getInputStream(), blobName);
            String error = localProcessLauncher.getOutputForProcess(LocalProcessLauncher.Output.ERROR);
            int exitCode = localProcessLauncher.waitForTerminate();

            if (exitCode != 0 && StringUtils.isNotBlank(error)) {
                LOGGER.error("process error: {}", error);
                return new BackupRestoreResponse(false, error);
            }
        }
        catch (LaunchProcessException ex){
            return new BackupRestoreResponse(false, ex.getMessage());
        }

        return new BackupRestoreResponse(true, "Backup successfully completed.");
    }

    public BackupRestoreResponse restoreAzureWDS(BackUpFileStorage storage, String version) {
        validateVersion(version);
        try {
            // grab blob from storage
            storage.downloadFromBlobStorage(backupName);
            
            // restore pgdump
            List<String> commandList = GenerateCommandList(false);
            commandList.add("-f" + backupName);
            Map<String, String> envVars = Map.of("PGPASSWORD", dbPassword);

            LocalProcessLauncher localProcessLauncher = new LocalProcessLauncher();
            localProcessLauncher.launchProcess(commandList, envVars);

            String error = localProcessLauncher.getOutputForProcess(LocalProcessLauncher.Output.ERROR);
            int exitCode = localProcessLauncher.waitForTerminate();
            if (exitCode != 0 && StringUtils.isNotBlank(error)) {
                LOGGER.error("process error: {}", error);
                return new BackupRestoreResponse(false, error);
            }

            // rename workspace from source to dest
            commandList.remove(commandList.size() - 1);
            //TODO: in the future this will shift to source and current workspace id 
            commandList.add(String.format("-c ALTER SCHEMA \"%s\" RENAME TO \"%s\"", workspaceId, "12345678-1234-1234-1234-123456789012"));
            localProcessLauncher.launchProcess(commandList, envVars);
            
            error = localProcessLauncher.getOutputForProcess(LocalProcessLauncher.Output.ERROR);
            exitCode = localProcessLauncher.waitForTerminate();
            if (exitCode != 0 && StringUtils.isNotBlank(error)) {
                LOGGER.error("process error: {}", error);
                return new BackupRestoreResponse(false, error);
            }
            
            // delete backup file
            Files.deleteIfExists(Paths.get(backupName));
            return new BackupRestoreResponse(true, "Successfully completed restore");
        } 
        catch (LaunchProcessException ex){
            return new BackupRestoreResponse(false, ex.getMessage());
        }
        catch(IOException ex) {
            return new BackupRestoreResponse(false, "Trouble deleting Azure Blob file, specifically:" + ex.getMessage());
        }
    }

    // Same args, different command depending on whether we're doing backup or restore.
    public List<String> GenerateCommandList(Boolean isBackup) {
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

    public String GenerateBackupFilename() {
        LocalDateTime now = LocalDateTime.now();
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd_HH-mm-ss");
        String timestamp = now.format(formatter);
        return workspaceId + "-" + timestamp + ".sql";
    }
}
