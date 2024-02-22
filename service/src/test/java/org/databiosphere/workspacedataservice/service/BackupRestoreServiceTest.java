package org.databiosphere.workspacedataservice.service;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;

import java.util.List;
import org.databiosphere.workspacedataservice.common.TestBase;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.TestPropertySource;

@DirtiesContext
@SpringBootTest(properties = "spring.cache.type=NONE")
@TestPropertySource(
    properties = {
      "twds.instance.workspace-id=f7c83b8d-53f3-473e-b9b4-2663d13e2752",
      "twds.pg_dump.path=/unit/test/pg_dump",
      "twds.pg_dump.psqlPath=/unit/test/psql"
    })
class BackupRestoreServiceTest extends TestBase {
  @Autowired private BackupRestoreService backupRestoreService;

  @Test
  void CheckBackupCommandLine() {
    List<String> commandList = backupRestoreService.generateCommandList(true);
    String command = String.join(" ", commandList);
    assertThat(command)
        .isEqualTo(
            "/unit/test/pg_dump -b -n f7c83b8d-53f3-473e-b9b4-2663d13e2752 -h localhost -p 5432 -U wds -d wds -v -w");
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
    assertThat(blobName)
        .containsPattern(
            "[0-9A-Fa-f]{8}-[0-9A-Fa-f]{4}-[0-9A-Fa-f]{4}-[0-9A-Fa-f]{4}-[0-9A-Fa-f]{12}-[0-9]{4}-[0-9]{2}-[0-9]{2}_([0-9]+(-[0-9]+)+)\\.sql");
  }
}
