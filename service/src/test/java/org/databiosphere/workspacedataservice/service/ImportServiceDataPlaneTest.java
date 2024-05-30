package org.databiosphere.workspacedataservice.service;

import static org.assertj.core.api.Assertions.assertThat;
import static org.databiosphere.workspacedataservice.generated.ImportRequestServerModel.TypeEnum.PFB;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import java.net.URI;
import java.util.UUID;
import org.broadinstitute.dsde.workbench.client.sam.ApiException;
import org.databiosphere.workspacedataservice.annotations.SingleTenant;
import org.databiosphere.workspacedataservice.common.TestBase;
import org.databiosphere.workspacedataservice.dao.CollectionDao;
import org.databiosphere.workspacedataservice.generated.ImportRequestServerModel;
import org.databiosphere.workspacedataservice.sam.MockSamAuthorizationDao;
import org.databiosphere.workspacedataservice.sam.SamAuthorizationDao;
import org.databiosphere.workspacedataservice.sam.SamAuthorizationDaoFactory;
import org.databiosphere.workspacedataservice.service.model.exception.AuthenticationException;
import org.databiosphere.workspacedataservice.service.model.exception.AuthenticationMaskableException;
import org.databiosphere.workspacedataservice.service.model.exception.CollectionException;
import org.databiosphere.workspacedataservice.service.model.exception.MissingObjectException;
import org.databiosphere.workspacedataservice.shared.model.CollectionId;
import org.databiosphere.workspacedataservice.shared.model.WorkspaceId;
import org.junit.jupiter.api.Test;
import org.mockito.stubbing.OngoingStubbing;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.dao.EmptyResultDataAccessException;
import org.springframework.http.HttpStatus;
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
  @MockBean SamAuthorizationDaoFactory samAuthorizationDaoFactory;
  private final SamAuthorizationDao samAuthorizationDao = spy(MockSamAuthorizationDao.allowAll());

  private final URI importUri =
      URI.create("https://teststorageaccount.blob.core.windows.net/testcontainer/file");
  private final ImportRequestServerModel importRequest =
      new ImportRequestServerModel(PFB, importUri);

  private final CollectionId collectionId = CollectionId.of(UUID.randomUUID());

  /* collection exists, workspace matches env var, user has write permission */
  @Test
  void userHasWritePermission() {
    // ARRANGE
    // collection dao says the collection exists and returns the expected workspace id
    when(collectionDao.collectionSchemaExists(collectionId.id())).thenReturn(true);
    when(collectionDao.getWorkspaceId(collectionId)).thenReturn(workspaceId);
    // sam dao says the user has write permission
    stubWriteWorkspacePermission(workspaceId).thenReturn(true);
    stubReadWorkspacePermission(workspaceId).thenReturn(true);

    // ACT/ASSERT
    // extract the UUID here so the lambda below has only one invocation possibly throwing a runtime
    // exception
    UUID collectionUuid = collectionId.id();
    // perform the import request
    assertDoesNotThrow(() -> importService.createImport(collectionUuid, importRequest));
  }

  /* collection exists, workspace matches env var, user has read but not write permission */
  @Test
  void userHasOnlyReadPermission() {
    // ARRANGE
    // collection dao says the collection exists and returns the expected workspace id
    when(collectionDao.collectionSchemaExists(collectionId.id())).thenReturn(true);
    when(collectionDao.getWorkspaceId(collectionId)).thenReturn(workspaceId);
    // sam dao says the user has read but not write permission
    stubWriteWorkspacePermission(workspaceId).thenReturn(false);
    stubReadWorkspacePermission(workspaceId).thenReturn(true);

    // ACT/ASSERT
    // extract the UUID here so the lambda below has only one invocation possibly throwing a runtime
    // exception
    UUID collectionUuid = collectionId.id();
    // perform the import request
    AuthenticationException actual =
        assertThrows(
            AuthenticationException.class,
            () -> importService.createImport(collectionUuid, importRequest));

    assertThat(actual)
        .withFailMessage("should not be a maskable exception")
        .isNotInstanceOf(AuthenticationMaskableException.class);
    assertEquals(HttpStatus.UNAUTHORIZED, actual.getStatusCode());
  }

  /* collection exists, workspace matches env var, user does not have access */
  @Test
  void userDoesNotHaveAccess() {
    // ARRANGE
    // collection dao says the collection exists and returns the expected workspace id
    when(collectionDao.collectionSchemaExists(collectionId.id())).thenReturn(true);
    when(collectionDao.getWorkspaceId(collectionId)).thenReturn(workspaceId);
    // sam dao says the user does not have read or write permission
    stubWriteWorkspacePermission(workspaceId).thenReturn(false);
    stubReadWorkspacePermission(workspaceId).thenReturn(false);

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

  @Test
  void errorCheckingReadAccess() {
    // ARRANGE
    // collection dao says the collection exists and returns the expected workspace id
    when(collectionDao.collectionSchemaExists(collectionId.id())).thenReturn(true);
    when(collectionDao.getWorkspaceId(collectionId)).thenReturn(workspaceId);
    // sam dao says the user does not have write permission, and fails to check read permission
    stubWriteWorkspacePermission(workspaceId).thenReturn(false);
    stubReadWorkspacePermission(workspaceId)
        .thenAnswer(
            invocation -> {
              throw new ApiException(0, "Unit test errors");
            });

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
    WorkspaceId nonMatchingWorkspaceId = WorkspaceId.of(UUID.randomUUID());
    // collection dao says the collection exists and returns an unexpected workspace id
    when(collectionDao.collectionSchemaExists(collectionId.id())).thenReturn(true);
    when(collectionDao.getWorkspaceId(collectionId)).thenReturn(nonMatchingWorkspaceId);
    // sam dao says the user has write permission
    stubWriteWorkspacePermission(nonMatchingWorkspaceId).thenReturn(true);

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

    // ACT/ASSERT
    // extract the UUID here so the lambda below has only one invocation possibly throwing a runtime
    // exception
    UUID collectionUuid = collectionId.id();
    // perform the import request
    assertThrows(
        MissingObjectException.class,
        () -> importService.createImport(collectionUuid, importRequest));

    verifyNoInteractions(samAuthorizationDaoFactory);
    verifyNoInteractions(samAuthorizationDao);
  }

  private OngoingStubbing<Boolean> stubWriteWorkspacePermission(WorkspaceId workspaceId) {
    when(samAuthorizationDaoFactory.getSamAuthorizationDao(workspaceId))
        .thenReturn(samAuthorizationDao);
    return when(samAuthorizationDao.hasWriteWorkspacePermission());
  }

  private OngoingStubbing<Boolean> stubReadWorkspacePermission(WorkspaceId workspaceId) {
    when(samAuthorizationDaoFactory.getSamAuthorizationDao(workspaceId))
        .thenReturn(samAuthorizationDao);
    return when(samAuthorizationDao.hasReadWorkspacePermission());
  }
}
