package org.databiosphere.workspacedataservice.service;

import org.databiosphere.workspacedataservice.storage.LocalFileStorage;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.MockBean;

import java.io.File;
import java.util.Scanner;

import static org.junit.jupiter.api.Assertions.assertTrue;

@SpringBootTest(properties = "spring.cache.type=NONE")
public class BackupServiceIntegrationTest {
    @Autowired
    private BackupService mockBackupService;

    private LocalFileStorage storage = new LocalFileStorage();

    @Test
    void testBackupAzureWDS() throws Exception {
        var response = mockBackupService.backupAzureWDS(storage, "v0.2");
        assertTrue(response.backupStatus());
    }
}