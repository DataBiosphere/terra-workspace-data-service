package org.databiosphere.workspacedataservice.service;

import org.apache.commons.lang3.StringUtils;
import org.databiosphere.workspacedataservice.process.LocalProcessLauncher;
import org.databiosphere.workspacedataservice.shared.model.BackupRestoreResponse;
import org.databiosphere.workspacedataservice.storage.LocalFileStorage;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.TestPropertySource;

import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.Map;


@SpringBootTest(properties = "spring.cache.type=NONE")
@TestPropertySource(
    properties = {
        "twds.instance.workspace-id=123e4567-e89b-12d3-a456-426614174000", 
        "twds.instance.source-workspace-id=123e4567-e89b-12d3-a456-426614174001", 
        "twds.pg_dump.port=5432",
        "twds.pg_dump.user=wds",
        "twds.pg_dump.dbName=wds",
        "twds.pg_dump.password=wds",
        "twds.pg_dump.host=localhost"
    }
)
public class RestoreServiceIntegrationTest {
    @Autowired
    private BackupRestoreService backupRestoreService;

    @Value("${twds.instance.workspace-id:}")
    private String workspaceId;

    @Value("${twds.pg_dump.password:}")
    private String password;

    private LocalFileStorage storage = new LocalFileStorage();
    
    @Test
    void testRestoreAzureWDS() throws Exception {
        // Clean database of workspace-id value
        cleanDatabase();
        var response = backupRestoreService.restoreAzureWDS(storage, "v0.2");
        assertTrue(response.backupRestoreStatus(), response.message());
    }

    private void cleanDatabase() {
        List<String> commandList = backupRestoreService.generateCommandList(false);
        commandList.add(String.format("-c DROP SCHEMA IF EXISTS \"%s\" CASCADE; DROP SCHEMA IF EXISTS \"%s\" CASCADE;", workspaceId, "sys_wds"));
        Map<String, String> envVars = Map.of("PGPASSWORD", password);
        LocalProcessLauncher localProcessLauncher = new LocalProcessLauncher();
        localProcessLauncher.launchProcess(commandList, envVars);
        String error = localProcessLauncher.getOutputForProcess(LocalProcessLauncher.Output.ERROR);
        int exitCode = localProcessLauncher.waitForTerminate();
        if (exitCode != 0 && StringUtils.isNotBlank(error)) {
            System.err.println("Error clearing database: " + error);
        }
    }
}

