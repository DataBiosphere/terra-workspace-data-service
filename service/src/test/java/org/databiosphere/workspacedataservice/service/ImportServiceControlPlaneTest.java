package org.databiosphere.workspacedataservice.service;

import static org.databiosphere.workspacedataservice.generated.ImportRequestServerModel.TypeEnum.*;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.when;

import java.net.URI;
import java.util.UUID;
import org.databiosphere.workspacedataservice.generated.ImportRequestServerModel;
import org.databiosphere.workspacedataservice.sam.SamDao;
import org.databiosphere.workspacedataservice.service.model.exception.AuthenticationMaskableException;
import org.databiosphere.workspacedataservice.shared.model.CollectionId;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.TestPropertySource;

/**
 * Tests for permission behaviors in the control plane. See also {@link ImportServiceTest} for tests
 * of functional correctness.
 *
 * @see ImportServiceTest
 */
@ActiveProfiles({"control-plane", "noop-scheduler-dao"})
@DirtiesContext
@SpringBootTest
@TestPropertySource(
    properties = {
      // TODO(AJ-1656): control-plane should not require instance config in any form, this is a hold
      //   over from direct injection of @Value('twds.instance.workspace-id')
      "twds.instance.workspace-id=",
    })
class ImportServiceControlPlaneTest {
  @Autowired ImportService importService;
  @MockBean SamDao samDao;
  private final URI importUri = URI.create("http://does/not/matter");
  private final ImportRequestServerModel importRequest =
      new ImportRequestServerModel(PFB, importUri);

  /* Collection does not exist, user has access */
  @Test
  void hasPermission() {
    // ARRANGE
    CollectionId collectionId = CollectionId.of(UUID.randomUUID());
    // sam dao says the user has write permission
    when(samDao.hasWriteWorkspacePermission(collectionId.toString())).thenReturn(true);

    // ACT/ASSERT
    // extract the UUID here so the lambda below has only one invocation possibly throwing a runtime
    // exception
    UUID collectionUuid = collectionId.id();
    // perform the import request
    assertDoesNotThrow(() -> importService.createImport(collectionUuid, importRequest));
  }

  /* Collection does not exist, user does not have access */
  @Test
  void doesNotHavePermission() {
    // ARRANGE
    CollectionId collectionId = CollectionId.of(UUID.randomUUID());
    // sam dao says the user does not have write permission
    when(samDao.hasWriteWorkspacePermission(collectionId.toString())).thenReturn(false);

    // ACT/ASSERT
    // extract the UUID here so the lambda below has only one invocation possibly throwing a runtime
    // exception
    UUID collectionUuid = collectionId.id();
    // perform the import request
    AuthenticationMaskableException actual =
        assertThrows(
            AuthenticationMaskableException.class,
            () -> importService.createImport(collectionUuid, importRequest));

    // ASSERT
    assertEquals("Collection", actual.getObjectType());
  }
}
