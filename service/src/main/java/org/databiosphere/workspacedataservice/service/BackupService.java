package org.databiosphere.workspacedataservice.service;

import org.apache.commons.lang3.StringUtils;
import org.databiosphere.workspacedataservice.dao.BackupDao;
import org.databiosphere.workspacedataservice.process.LocalProcessLauncher;
import org.databiosphere.workspacedataservice.service.model.exception.LaunchProcessException;
import org.databiosphere.workspacedataservice.shared.model.BackupResponse;
import org.databiosphere.workspacedataservice.storage.BackUpFileStorage;
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

    public BackupService(BackupDao backupDao) {
        this.backupDao = backupDao;
    }

    public BackupResponse checkBackupStatus(String trackingId) {

        return new BackupResponse(true, "Backup successfully completed.");
    }

    public void backupAzureWDS(BackUpFileStorage storage, String version, String trackingId) {
        try {
            validateVersion(version);
            String blobName = GenerateBackupFilename();

            List<String> commandList = GenerateCommandList();
            Map<String, String> envVars = Map.of("PGPASSWORD", dbPassword);

            LocalProcessLauncher localProcessLauncher = new LocalProcessLauncher();
            localProcessLauncher.launchProcess(commandList, envVars);

            storage.streamOutputToBlobStorage(localProcessLauncher.getInputStream(), blobName);
            String error = localProcessLauncher.getOutputForProcess(LocalProcessLauncher.Output.ERROR);
            int exitCode = localProcessLauncher.waitForTerminate();

            if (exitCode != 0 && StringUtils.isNotBlank(error)) {
                LOGGER.error("process error: {}", error);
            }
        }
        catch (LaunchProcessException ex){
            LOGGER.error("process error: {}", ex);
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
        return workspaceId + "-" + timestamp + ".sql";
    }
}

