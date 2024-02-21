package org.databiosphere.workspacedataservice.service;

import static org.databiosphere.workspacedataservice.generated.ImportRequestServerModel.TypeEnum.PFB;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.when;

import java.net.URI;
import java.util.UUID;
import org.databiosphere.workspacedataservice.config.TwdsProperties;
import org.databiosphere.workspacedataservice.dao.CollectionDao;
import org.databiosphere.workspacedataservice.generated.ImportRequestServerModel;
import org.databiosphere.workspacedataservice.sam.SamDao;
import org.databiosphere.workspacedataservice.service.model.exception.AuthenticationMaskableException;
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
@ActiveProfiles({"data-plane", "noop-scheduler-dao"})
@DirtiesContext
// the "data-plane" profile enforces validity of twds.instance.workspace-id, so we need to set that
@SpringBootTest(properties = {"twds.instance.workspace-id=b01dface-0000-0000-0000-000000000000"})
class ImportServiceDataPlaneTest {

  @Autowired ImportService importService;
  @Autowired TwdsProperties twdsProperties;
  @MockBean CollectionDao collectionDao;
  @MockBean SamDao samDao;

  private final CollectionId collectionId = CollectionId.of(UUID.randomUUID());

  /* collection exists, workspace matches env var, user has access */
  @Test
  void userHasAccess() {
    // ARRANGE
    WorkspaceId workspaceId = WorkspaceId.of(twdsProperties.getInstance().getWorkspaceUuid());
    // collection dao says the collection exists and returns the expected workspace id
    when(collectionDao.collectionSchemaExists(collectionId.id())).thenReturn(true);
    when(collectionDao.getWorkspaceId(collectionId)).thenReturn(workspaceId);
    // sam dao says the user has write permission
    when(samDao.hasWriteWorkspacePermission(workspaceId.toString())).thenReturn(true);

    // define the import request
    URI importUri = URI.create("http://does/not/matter");
    ImportRequestServerModel importRequest = new ImportRequestServerModel(PFB, importUri);

    // ACT/ASSERT
    // extract the UUID here so the lambda below has only one invocation possibly throwing a runtime
    // exception
    UUID collectionUuid = collectionId.id();
    // perform the import request
    assertDoesNotThrow(() -> importService.createImport(collectionUuid, importRequest));
  }

  /* collection exists, workspace matches env var, user does not have access */
  @Test
  void userDoesNotHaveAccess() {
    // ARRANGE
    WorkspaceId workspaceId = WorkspaceId.of(twdsProperties.getInstance().getWorkspaceUuid());
    // collection dao says the collection exists and returns the expected workspace id
    when(collectionDao.collectionSchemaExists(collectionId.id())).thenReturn(true);
    when(collectionDao.getWorkspaceId(collectionId)).thenReturn(workspaceId);
    // sam dao says the user has write permission
    when(samDao.hasWriteWorkspacePermission(workspaceId.toString())).thenReturn(false);

    // define the import request
    URI importUri = URI.create("http://does/not/matter");
    ImportRequestServerModel importRequest = new ImportRequestServerModel(PFB, importUri);

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

  /* collection exists, workspace does not match env var */
  @Test
  void unexpectedWorkspaceId() {
    // ARRANGE
    WorkspaceId workspaceId = WorkspaceId.of(UUID.randomUUID());
    // collection dao says the collection exists and returns an unexpected workspace id
    when(collectionDao.collectionSchemaExists(collectionId.id())).thenReturn(true);
    when(collectionDao.getWorkspaceId(collectionId)).thenReturn(workspaceId);
    // sam dao says the user has write permission
    when(samDao.hasWriteWorkspacePermission(workspaceId.toString())).thenReturn(false);

    // define the import request
    URI importUri = URI.create("http://does/not/matter");
    ImportRequestServerModel importRequest = new ImportRequestServerModel(PFB, importUri);

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
    when(collectionDao.collectionSchemaExists(collectionId.id())).thenReturn(false);
    when(collectionDao.getWorkspaceId(collectionId))
        .thenThrow(new EmptyResultDataAccessException("unit test intentional error", 1));
    // sam dao says the user has write permission
    when(samDao.hasWriteWorkspacePermission(collectionId.toString())).thenReturn(false);

    // define the import request
    URI importUri = URI.create("http://does/not/matter");
    ImportRequestServerModel importRequest = new ImportRequestServerModel(PFB, importUri);

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
