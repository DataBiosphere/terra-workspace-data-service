package org.databiosphere.workspacedataservice.service;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doThrow;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;
import org.databiosphere.workspacedataservice.common.DataPlaneTestBase;
import org.databiosphere.workspacedataservice.dao.BackupRestoreDao;
import org.databiosphere.workspacedataservice.service.model.exception.LaunchProcessException;
import org.databiosphere.workspacedataservice.shared.model.RestoreResponse;
import org.databiosphere.workspacedataservice.shared.model.WorkspaceId;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.TestPropertySource;

@DirtiesContext
@SpringBootTest(properties = "spring.cache.type=NONE")
@TestPropertySource(
    properties = {"twds.pg_dump.path=/unit/test/pg_dump", "twds.pg_dump.psqlPath=/unit/test/psql"})
class BackupRestoreServiceTest extends DataPlaneTestBase {
  @Autowired private BackupRestoreService backupRestoreService;
  @Autowired private NamedParameterJdbcTemplate namedTemplate;
  @Autowired private WorkspaceId workspaceId;

  @MockBean BackupRestoreDao<RestoreResponse> mockRestoreDao;

  @Test
  void CheckBackupCommandLine() {
    String expectedCommand =
        "/unit/test/pg_dump -b -n %s -h localhost -p 5432 -U wds -d wds -v -w"
            .formatted(workspaceId);
    List<String> commandList = backupRestoreService.generateCommandList(true);
    String command = String.join(" ", commandList);
    assertThat(command).isEqualTo(expectedCommand);
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

  @Test
  void postgresSearchPathResetToDefault() {
    // ARRANGE
    // make note of the Postgres search path, prior to doing anything else with restores
    String beforeCloneSearchPath =
        namedTemplate.queryForObject("SHOW search_path;", Map.of(), String.class);
    assertNotNull(beforeCloneSearchPath);

    // simulate the behavior of restoring a pg_dump:
    UUID trackingId = UUID.randomUUID();

    // pg_dumps contain the following `set_config` line, which changes the Postgres search path.
    // manually execute it here, simulating what happens when we restore the pg_dump
    namedTemplate.queryForObject(
        "SELECT pg_catalog.set_config('search_path', '', false);", Map.of(), String.class);
    // for the purposes of this test, since we've simulated the search path change, no need to do
    // anything else. Throw an error from the RestoreDao, so we will exit out of `restoreAzureWDS`
    // early.
    // N.B. exiting early makes this test much faster, because it exits before we try to use Azure
    // credentials to generate tokens.
    doThrow(new LaunchProcessException("Unit test intentional error"))
        .when(mockRestoreDao)
        .createEntry(eq(trackingId), any());

    // ACT
    // Execute a restore. This restore will fail due to the mocking above; that's ok - this test
    // only verifies that the restore code resets the Postgres search path (even on failure)
    backupRestoreService.restoreAzureWDS(
        "v0.2", "/file/name/is/irrelevant", trackingId, "token-is-irrelevant");

    // ASSERT
    // make note of the Postgres search path, after cloning
    String afterCloneSearchPath =
        namedTemplate.queryForObject("SHOW search_path;", Map.of(), String.class);
    assertNotNull(afterCloneSearchPath);

    // assert the postgres search path after restore is the same as before restore
    assertEquals(searchPathToSet(beforeCloneSearchPath), searchPathToSet(afterCloneSearchPath));
  }

  // Postgres search path is a comma-delimited list; change it to a Set because we don't care
  // about order or duplication of items within the search path
  private Set<String> searchPathToSet(String searchPath) {
    return Arrays.stream(searchPath.split(",")).map(String::trim).collect(Collectors.toSet());
  }
}
