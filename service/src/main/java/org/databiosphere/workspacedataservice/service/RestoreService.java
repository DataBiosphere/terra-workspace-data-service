package org.databiosphere.workspacedataservice.service;

import org.databiosphere.workspacedataservice.shared.model.BackupResponse;
import org.databiosphere.workspacedataservice.shared.model.RestoreResponse;
import org.databiosphere.workspacedataservice.storage.BackUpFileStorage;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import static org.databiosphere.workspacedataservice.service.RecordUtils.validateVersion;

import java.util.*;

import org.databiosphere.workspacedataservice.process.LocalProcessLauncher;
import org.databiosphere.workspacedataservice.service.model.exception.LaunchProcessException;

@Service
public class RestoreService {

    private final String backupName = "backup.sql";

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

    @Value("${twds.pg_dump.psqlPath:}")
    private String psqlPath;

    public RestoreResponse restoreAzureWDS(BackUpFileStorage storage, String version) {
        validateVersion(version);
        try {
            // TODO grab blob from storage
            storage.streamInputFromBlobStorage(backupName);
            
            // TODO rename workspace from source to dest
            // TODO pgdump restore
            List<String> commandList = GenerateCommandList();
            Map<String, String> envVars = Map.of("PGPASSWORD", dbPassword);

            LocalProcessLauncher localProcessLauncher = new LocalProcessLauncher();
            localProcessLauncher.launchProcess(commandList, envVars);

            // TODO delete backup file
        return new RestoreResponse(true, "Successfully completed restore");
        } 
        catch (LaunchProcessException ex){
            return new RestoreResponse(false, ex.getMessage());
        }
    }

    public List<String> GenerateCommandList() {
        Map<String, String> command = new LinkedHashMap<>();
        command.put(psqlPath, null);
        command.put("-h", dbHost);
        command.put("-p", dbPort);
        command.put("-U", dbUser);
        command.put("-d", dbName);
        command.put("-f", backupName);

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
}
