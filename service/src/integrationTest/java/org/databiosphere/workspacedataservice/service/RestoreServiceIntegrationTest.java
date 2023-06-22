package org.databiosphere.workspacedataservice.service;

import org.databiosphere.workspacedataservice.storage.LocalFileStorage;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.TestPropertySource;

import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;


@SpringBootTest(properties = "spring.cache.type=NONE")
@TestPropertySource(
    properties = {
        "twds.instance.workspace-id=123e4567-e89b-12d3-a456-426614174000", 
        "twds.instance.source-workspace-id=123e4567-e89b-12d3-a456-426614174001", 
        "twds.pg_dump.port=5432",
        "twds.pg_dump.user=wds",
        "twds.pg_dump.dbName=wds",
        "twds.pg_dump.host=localhost"
    }
)
public class RestoreServiceIntegrationTest {
    @Autowired
    private BackupRestoreService backupRestoreService;

    private LocalFileStorage storage = new LocalFileStorage();
    
    @Test
    void testRestoreAzureWDS() throws Exception {
        // Create the local pg_dump file by calling backup first.
        backupRestoreService.backupAzureWDS(storage, "v0.2");
        // Rename GUID in file to be "source" 
        modifySourceWorkspaceId();
        var response = backupRestoreService.restoreAzureWDS(storage, "v0.2");
        assertTrue(response.backupRestoreStatus(), response.message());
    }

    private void modifySourceWorkspaceId() {
        // We know the pg_dump will always be "backup.sql"
        try {
            Path path = Paths.get("backup.sql");
            String content = new String(Files.readAllBytes(path), StandardCharsets.UTF_8);
            content = content.replaceAll("123e4567-e89b-12d3-a456-426614174000", "123e4567-e89b-12d3-a456-426614174001");
            Files.write(path, content.getBytes(StandardCharsets.UTF_8));
        }
        catch(IOException ex) {
            System.err.format("IOException in RestoreServiceIntegrationTest.testRestoreAzureWDS: %s%n", ex);
        }
    }
}

