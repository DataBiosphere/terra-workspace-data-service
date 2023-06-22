package org.databiosphere.workspacedataservice.service;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.TestPropertySource;

import java.util.List;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;

@SpringBootTest(properties = "spring.cache.type=NONE")
@TestPropertySource(properties = {"twds.instance.workspace-id=123e4567-e89b-12d3-a456-426614174000"})
public class RestoreServiceTest {
    @Autowired
    private BackupRestoreService backupRestoreService;

    @Test
    void CheckCommandLine() {
        List<String> commandList = backupRestoreService.generateCommandList(true);
        String command = String.join(" ", commandList);
        assertThat(command).isEqualTo("/usr/bin/psql -h localhost -p 5432 -U wds -d wds -v -w");
    }
}
