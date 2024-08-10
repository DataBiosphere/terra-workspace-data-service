package org.databiosphere.workspacedataservice.service;

import static org.databiosphere.workspacedataservice.generated.ImportRequestServerModel.TypeEnum.PFB;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.when;

import java.net.URI;
import java.util.UUID;
import org.databiosphere.workspacedataservice.annotations.SingleTenant;
import org.databiosphere.workspacedataservice.common.TestBase;
import org.databiosphere.workspacedataservice.dao.CollectionDao;
import org.databiosphere.workspacedataservice.generated.ImportRequestServerModel;
import org.databiosphere.workspacedataservice.service.model.exception.CollectionException;
import org.databiosphere.workspacedataservice.service.model.exception.MissingObjectException;
import org.databiosphere.workspacedataservice.shared.model.CollectionId;
import org.databiosphere.workspacedataservice.shared.model.WorkspaceId;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.dao.EmptyResultDataAccessException;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.ActiveProfiles;

/**
 * Tests for permission behaviors in the data plane. See also {@link ImportServiceTest} for tests of
 * functional correctness.
 *
 * @see ImportServiceTest
 */
@ActiveProfiles({"data-plane", "noop-scheduler-dao", "mock-sam"})
@DirtiesContext
@SpringBootTest
class ImportServiceDataPlaneTest extends TestBase {
  @Autowired ImportService importService;
  @Autowired @SingleTenant WorkspaceId workspaceId;
  @MockBean CollectionDao collectionDao;

  private final URI importUri =
      URI.create("https://teststorageaccount.blob.core.windows.net/testcontainer/file");
  private final ImportRequestServerModel importRequest =
      new ImportRequestServerModel(PFB, importUri);

  private final CollectionId collectionId = CollectionId.of(UUID.randomUUID());

  /* collection exists, workspace matches env var */
  @Test
  void singleTenantWorkspaceId() {
    // ARRANGE
    // collection dao says the collection exists and returns the expected workspace id
    when(collectionDao.collectionSchemaExists(collectionId)).thenReturn(true);
    when(collectionDao.getWorkspaceId(collectionId)).thenReturn(workspaceId);

    // ACT/ASSERT
    // extract the UUID here so the lambda below has only one invocation possibly throwing a runtime
    // exception
    UUID collectionUuid = collectionId.id();
    // perform the import request
    assertDoesNotThrow(() -> importService.createImport(collectionUuid, importRequest));
  }

  /* collection exists, workspace does not match env var */
  @Test
  void unexpectedWorkspaceId() {
    // ARRANGE
    WorkspaceId nonMatchingWorkspaceId = WorkspaceId.of(UUID.randomUUID());
    // collection dao says the collection exists and returns an unexpected workspace id
    when(collectionDao.collectionSchemaExists(collectionId)).thenReturn(true);
    when(collectionDao.getWorkspaceId(collectionId)).thenReturn(nonMatchingWorkspaceId);

    // ACT/ASSERT
    // extract the UUID here so the lambda below has only one invocation possibly throwing a runtime
    // exception
    UUID collectionUuid = collectionId.id();
    // perform the import request
    assertThrows(
        CollectionException.class, () -> importService.createImport(collectionUuid, importRequest));
  }

  /* collection does not exist */
  @Test
  void collectionDoesNotExist() {
    // ARRANGE
    // collection dao says the collection does not exist
    when(collectionDao.collectionSchemaExists(collectionId)).thenReturn(false);
    when(collectionDao.getWorkspaceId(collectionId))
        .thenThrow(new EmptyResultDataAccessException("unit test intentional error", 1));

    // ACT/ASSERT
    // extract the UUID here so the lambda below has only one invocation possibly throwing a runtime
    // exception
    UUID collectionUuid = collectionId.id();
    // perform the import request
    assertThrows(
        MissingObjectException.class,
        () -> importService.createImport(collectionUuid, importRequest));
  }
}
