package org.databiosphere.workspacedataservice.service;

import static org.databiosphere.workspacedataservice.generated.ImportRequestServerModel.TypeEnum.PFB;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.when;

import java.net.URI;
import java.util.UUID;
import org.databiosphere.workspacedataservice.annotations.SingleTenant;
import org.databiosphere.workspacedataservice.common.DataPlaneTestBase;
import org.databiosphere.workspacedataservice.generated.ImportRequestServerModel;
import org.databiosphere.workspacedataservice.service.model.exception.CollectionException;
import org.databiosphere.workspacedataservice.service.model.exception.MissingObjectException;
import org.databiosphere.workspacedataservice.shared.model.CollectionId;
import org.databiosphere.workspacedataservice.shared.model.WorkspaceId;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.MockBean;
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
class ImportServiceDataPlaneTest extends DataPlaneTestBase {
  @Autowired ImportService importService;
  @Autowired @SingleTenant WorkspaceId workspaceId;
  @MockBean CollectionService collectionService;

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
    // collectionService.validateCollection() does not throw
    when(collectionService.getWorkspaceId(collectionId)).thenReturn(workspaceId);

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
    // collection dao says the collection exists and returns an unexpected workspace id
    // collectionService.validateCollection() does not throw
    when(collectionService.getWorkspaceId(collectionId))
        .thenThrow(new CollectionException("Found unexpected workspaceId for collection"));

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
    doThrow(new MissingObjectException("Collection"))
        .when(collectionService)
        .getWorkspaceId(collectionId);

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
