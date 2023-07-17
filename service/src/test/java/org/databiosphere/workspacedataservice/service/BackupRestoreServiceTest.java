package org.databiosphere.workspacedataservice.service;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.TestPropertySource;

import java.util.List;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;

@SpringBootTest(properties = "spring.cache.type=NONE")
@TestPropertySource(properties = {"twds.instance.workspace-id=123e4567-e89b-12d3-a456-426614174000",
        "twds.pg_dump.path=/unit/test/pg_dump",
        "twds.pg_dump.psqlPath=/unit/test/psql"})
class BackupRestoreServiceTest {
    @Autowired
    private BackupRestoreService backupRestoreService;

    @Test
    void CheckBackupCommandLine() {
        List<String> commandList = backupRestoreService.generateCommandList(true);
        String command = String.join(" ", commandList);
        assertThat(command).isEqualTo("/unit/test/pg_dump -b -n 123e4567-e89b-12d3-a456-426614174000 -h localhost -p 5432 -U wds -d wds -v -w");
    }

    @Test
    void CheckRestoreCommandLine() {
        List<String> commandList = backupRestoreService.generateCommandList(false);
        String command = String.join(" ", commandList);
        assertThat(command).isEqualTo("/unit/test/psql -h localhost -p 5432 -U wds -d wds -v -w");
    }

    @Test
    void VerifyBackupFileName() {
        String blobName = backupRestoreService.generateBackupFilename();
        assertThat(blobName).containsPattern("[0-9A-Fa-f]{8}-[0-9A-Fa-f]{4}-[0-9A-Fa-f]{4}-[0-9A-Fa-f]{4}-[0-9A-Fa-f]{12}-[0-9]{4}-[0-9]{2}-[0-9]{2}_([0-9]+(-[0-9]+)+)\\.sql");
    }
}
