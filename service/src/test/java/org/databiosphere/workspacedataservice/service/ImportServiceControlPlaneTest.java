package org.databiosphere.workspacedataservice.service;

import static org.databiosphere.workspacedataservice.generated.ImportRequestServerModel.TypeEnum.*;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.when;

import java.net.URI;
import java.util.UUID;
import org.databiosphere.workspacedataservice.dao.CollectionDao;
import org.databiosphere.workspacedataservice.generated.ImportRequestServerModel;
import org.databiosphere.workspacedataservice.sam.SamAuthorizationDao;
import org.databiosphere.workspacedataservice.service.model.exception.AuthenticationMaskableException;
import org.databiosphere.workspacedataservice.service.model.exception.CollectionException;
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
 * Tests for permission behaviors in the control plane. See also {@link ImportServiceTest} for tests
 * of functional correctness.
 *
 * @see ImportServiceTest
 */
@ActiveProfiles({"control-plane", "noop-scheduler-dao"})
@DirtiesContext
@SpringBootTest
class ImportServiceControlPlaneTest {

  @Autowired ImportService importService;
  @MockBean CollectionDao collectionDao;
  @MockBean SamAuthorizationDao samAuthorizationDao;
  private final URI importUri = URI.create("http://does/not/matter");
  private final ImportRequestServerModel importRequest =
      new ImportRequestServerModel(PFB, importUri);

  /* Collection does not exist, user has access */
  @Test
  void hasPermission() {
    // ARRANGE
    CollectionId collectionId = CollectionId.of(UUID.randomUUID());
    // collection dao says the collection does not exist
    when(collectionDao.getWorkspaceId(collectionId))
        .thenThrow(new EmptyResultDataAccessException("unit test intentional error", 1));
    // sam dao says the user has write permission
    when(samAuthorizationDao.hasWriteWorkspacePermission(WorkspaceId.of(collectionId.id())))
        .thenReturn(true);

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
    // collection dao says the collection does not exist
    when(collectionDao.getWorkspaceId(collectionId))
        .thenThrow(new EmptyResultDataAccessException("unit test intentional error", 1));
    // sam dao says the user does not have write permission
    when(samAuthorizationDao.hasWriteWorkspacePermission(WorkspaceId.of(collectionId.id())))
        .thenReturn(false);

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

  /* Collection exists, which is an error in the control plane */
  @Test
  void collectionExists() {
    // ARRANGE
    CollectionId collectionId = CollectionId.of(UUID.randomUUID());
    // collection dao says the collection DOES exist, which is an error in the control plane
    when(collectionDao.getWorkspaceId(collectionId)).thenReturn(WorkspaceId.of(UUID.randomUUID()));
    // sam dao says the user does have write permission
    when(samAuthorizationDao.hasWriteWorkspacePermission(WorkspaceId.of(collectionId.id())))
        .thenReturn(true);

    // ACT/ASSERT
    // extract the UUID here so the lambda below has only one invocation possibly throwing a runtime
    // exception
    UUID collectionUuid = collectionId.id();
    // perform the import request
    assertThrows(
        CollectionException.class, () -> importService.createImport(collectionUuid, importRequest));
  }
}
