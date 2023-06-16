package org.databiosphere.workspacedataservice.service;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.TestPropertySource;

import java.util.List;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;

@SpringBootTest(properties = "spring.cache.type=NONE")
@TestPropertySource(properties = {"twds.instance.workspace-id=123e4567-e89b-12d3-a456-426614174000"})
class BackupServiceTest {
    @Autowired
    private BackupService backupService;

    @Test
    void CheckCommandLine() {
        List<String> commandList = backupService.GenerateCommandList();
        String command = String.join(" ", commandList);
        assertThat(command).isEqualTo("/usr/bin/pg_dump -h localhost -p 5432 -U wds -d wds -v -w");
    }

    @Test
    void VerifyBackupFileName() {
        String blobName = backupService.GenerateBackupFilename();
        assertThat(blobName).containsPattern("[0-9A-Fa-f]{8}-[0-9A-Fa-f]{4}-[0-9A-Fa-f]{4}-[0-9A-Fa-f]{4}-[0-9A-Fa-f]{12}-[0-9]{4}-[0-9]{2}-[0-9]{2}_([0-9]+(-[0-9]+)+)\\.sql");
    }
}
