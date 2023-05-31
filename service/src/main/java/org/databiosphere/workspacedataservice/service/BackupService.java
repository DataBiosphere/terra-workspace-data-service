package org.databiosphere.workspacedataservice.service;

import bio.terra.common.db.WriteTransaction;
import org.apache.commons.lang3.StringUtils;
import org.databiosphere.workspacedataservice.process.LocalProcessLauncher;
import org.databiosphere.workspacedataservice.service.model.exception.LaunchProcessException;
import org.databiosphere.workspacedataservice.storage.BackUpFileStorage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;

@Service
public class BackupService {
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

    @WriteTransaction
    public boolean backupAzureWDS(BackUpFileStorage storage) {
        String blobName = GenerateBackupFilename(workspaceId);

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

        Map<String, String> envVars = Map.of("PGPASSWORD", dbPassword);

        LocalProcessLauncher localProcessLauncher = new LocalProcessLauncher();
        localProcessLauncher.launchProcess(commandList, envVars);
        storage.streamOutputToBlobStorage(localProcessLauncher.getInputStream(), blobName);

        String error = localProcessLauncher.getOutputForProcess(LocalProcessLauncher.Output.ERROR);
        int exitCode = localProcessLauncher.waitForTerminate();

        if (exitCode != 0 && StringUtils.isNotBlank(error)) {
            LOGGER.error("process error: {}", error);
            return false;
        }

        return true;
    }

    public static String GenerateBackupFilename(String workspaceId) {
        LocalDateTime now = LocalDateTime.now();
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd_HH-mm-ss");
        String timestamp = now.format(formatter);
        return  workspaceId + "-" + timestamp + ".sql";
    }
}
