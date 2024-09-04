package org.databiosphere.workspacedataservice.service;

import static org.databiosphere.workspacedataservice.generated.ImportRequestServerModel.TypeEnum.*;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.mockito.Mockito.when;

import java.net.URI;
import java.util.UUID;
import org.databiosphere.workspacedataservice.common.ControlPlaneTestBase;
import org.databiosphere.workspacedataservice.generated.ImportRequestServerModel;
import org.databiosphere.workspacedataservice.shared.model.CollectionId;
import org.databiosphere.workspacedataservice.shared.model.WorkspaceId;
import org.databiosphere.workspacedataservice.workspace.DataTableTypeInspector;
import org.databiosphere.workspacedataservice.workspace.WorkspaceDataTableType;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.ActiveProfiles;

/**
 * Tests for permission behaviors in the control plane. See also {@link ImportServiceTest} for tests
 * of functional correctness.
 *
 * @see ImportServiceTest
 */
@ActiveProfiles({"noop-scheduler-dao", "mock-sam"})
@DirtiesContext
@SpringBootTest
class ImportServiceControlPlaneTest extends ControlPlaneTestBase {

  @Autowired ImportService importService;

  @MockBean DataTableTypeInspector dataTableTypeInspector;

  private final URI importUri =
      URI.create("https://teststorageaccount.blob.core.windows.net/testcontainer/file");
  private final ImportRequestServerModel importRequest =
      new ImportRequestServerModel(PFB, importUri);

  /* Collection does not exist, which is expected in the control plane */
  @Test
  void virtualCollection() {
    // ARRANGE
    CollectionId collectionId = CollectionId.of(UUID.randomUUID());
    // do not create a collection; we want to test virtual collections here.
    // mock out the DataTableTypeInspector to show that this workspace is Rawls-powered.
    when(dataTableTypeInspector.getWorkspaceDataTableType(WorkspaceId.of(collectionId.id())))
        .thenReturn(WorkspaceDataTableType.RAWLS);

    // ACT/ASSERT
    // extract the UUID here so the lambda below has only one invocation possibly throwing a runtime
    // exception
    UUID collectionUuid = collectionId.id();
    // perform the import request
    assertDoesNotThrow(() -> importService.createImport(collectionUuid, importRequest));
  }
}
