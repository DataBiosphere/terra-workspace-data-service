package org.databiosphere.workspacedataservice.service;

import org.databiosphere.workspacedataservice.dao.InstanceDao;
import org.databiosphere.workspacedataservice.storage.LocalFileStorage;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.TestPropertySource;

import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.UUID;

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

    @Autowired
	InstanceDao instanceDao;

    @Value("${twds.instance.workspace-id:}")
    private String workspaceId;

    private LocalFileStorage storage = new LocalFileStorage();
    
    @Test
    void testRestoreAzureWDS() throws Exception {
        // Clean database of workspace-id value
        instanceDao.dropSchema(UUID.fromString(workspaceId));
        var response = backupRestoreService.restoreAzureWDS(storage, "v0.2");
        assertTrue(response.backupRestoreStatus(), response.message());
    }
}

