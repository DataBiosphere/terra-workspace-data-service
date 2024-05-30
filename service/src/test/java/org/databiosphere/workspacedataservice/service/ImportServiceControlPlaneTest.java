package org.databiosphere.workspacedataservice.service;

import static org.assertj.core.api.Assertions.assertThat;
import static org.databiosphere.workspacedataservice.generated.ImportRequestServerModel.TypeEnum.*;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

import java.net.URI;
import java.util.UUID;
import org.broadinstitute.dsde.workbench.client.sam.ApiException;
import org.databiosphere.workspacedataservice.dao.CollectionDao;
import org.databiosphere.workspacedataservice.generated.ImportRequestServerModel;
import org.databiosphere.workspacedataservice.sam.MockSamAuthorizationDao;
import org.databiosphere.workspacedataservice.sam.SamAuthorizationDao;
import org.databiosphere.workspacedataservice.sam.SamAuthorizationDaoFactory;
import org.databiosphere.workspacedataservice.service.model.exception.AuthenticationException;
import org.databiosphere.workspacedataservice.service.model.exception.AuthenticationMaskableException;
import org.databiosphere.workspacedataservice.service.model.exception.CollectionException;
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
import org.springframework.test.context.TestPropertySource;

/**
 * Tests for permission behaviors in the control plane. See also {@link ImportServiceTest} for tests
 * of functional correctness.
 *
 * @see ImportServiceTest
 */
@ActiveProfiles({"control-plane", "noop-scheduler-dao", "mock-sam"})
@DirtiesContext
@SpringBootTest
@TestPropertySource(
    properties = {
      // turn off pubsub autoconfiguration for tests
      "spring.cloud.gcp.pubsub.enabled=false",
      // Rawls url must be valid, else context initialization (Spring startup) will fail
      "rawlsUrl=https://localhost/"
    })
class ImportServiceControlPlaneTest {

  @Autowired ImportService importService;
  @MockBean CollectionDao collectionDao;
  @MockBean SamAuthorizationDaoFactory samAuthorizationDaoFactory;

  private final SamAuthorizationDao samAuthorizationDao = spy(MockSamAuthorizationDao.allowAll());
  private final URI importUri =
      URI.create("https://teststorageaccount.blob.core.windows.net/testcontainer/file");
  private final ImportRequestServerModel importRequest =
      new ImportRequestServerModel(PFB, importUri);

  /* Collection does not exist, user has access */
  @Test
  void hasWritePermission() {
    // ARRANGE
    CollectionId collectionId = CollectionId.of(UUID.randomUUID());
    // collection dao says the collection does not exist
    when(collectionDao.getWorkspaceId(collectionId))
        .thenThrow(new EmptyResultDataAccessException("unit test intentional error", 1));
    // sam dao says the user has write permission
    stubWriteWorkspacePermission(WorkspaceId.of(collectionId.id())).thenReturn(true);

    // ACT/ASSERT
    // perform the import request
    assertDoesNotThrow(() -> importService.createImport(collectionId, importRequest));
  }

  /* Collection does not exist, user does not have access */
  @Test
  void doesNotHavePermission() {
    // ARRANGE
    CollectionId collectionId = CollectionId.of(UUID.randomUUID());
    // collection dao says the collection does not exist
    when(collectionDao.getWorkspaceId(collectionId))
        .thenThrow(new EmptyResultDataAccessException("unit test intentional error", 1));
    // sam dao says the user has neither write nor read permission
    stubWriteWorkspacePermission(WorkspaceId.of(collectionId.id())).thenReturn(false);
    stubReadWorkspacePermission(WorkspaceId.of(collectionId.id())).thenReturn(false);

    // ACT/ASSERT
    // perform the import request
    AuthenticationMaskableException actual =
        assertThrows(
            AuthenticationMaskableException.class,
            () -> importService.createImport(collectionId, importRequest));

    // ASSERT
    assertEquals("Collection", actual.getObjectType());
  }

  @Test
  void errorCheckingReadAccess() {
    // ARRANGE
    CollectionId collectionId = CollectionId.of(UUID.randomUUID());
    // collection dao says the collection does not exist
    when(collectionDao.getWorkspaceId(collectionId))
        .thenThrow(new EmptyResultDataAccessException("unit test intentional error", 1));
    // sam dao says the user has neither write nor read permission
    stubWriteWorkspacePermission(WorkspaceId.of(collectionId.id())).thenReturn(false);
    stubReadWorkspacePermission(WorkspaceId.of(collectionId.id()))
        .thenAnswer(
            invocation -> {
              throw new ApiException(0, "Unit test errors");
            });

    // ACT/ASSERT
    // perform the import request
    AuthenticationMaskableException actual =
        assertThrows(
            AuthenticationMaskableException.class,
            () -> importService.createImport(collectionId, importRequest));

    // ASSERT
    assertEquals("Collection", actual.getObjectType());
  }

  @Test
  void hasOnlyReadPermission() {
    // ARRANGE
    CollectionId collectionId = CollectionId.of(UUID.randomUUID());
    // collection dao says the collection does not exist
    when(collectionDao.getWorkspaceId(collectionId))
        .thenThrow(new EmptyResultDataAccessException("unit test intentional error", 1));
    // sam dao says the user has neither write nor read permission
    stubWriteWorkspacePermission(WorkspaceId.of(collectionId.id())).thenReturn(false);
    stubReadWorkspacePermission(WorkspaceId.of(collectionId.id())).thenReturn(true);

    // ACT/ASSERT
    // perform the import request
    AuthenticationException actual =
        assertThrows(
            AuthenticationException.class,
            () -> importService.createImport(collectionId, importRequest));

    // ASSERT
    assertThat(actual)
        .withFailMessage("should not be a maskable exception")
        .isNotInstanceOf(AuthenticationMaskableException.class);
    assertEquals(HttpStatus.UNAUTHORIZED, actual.getStatusCode());
  }

  /* Collection exists, which is an error in the control plane */
  @Test
  void collectionExists() {
    // ARRANGE
    CollectionId collectionId = CollectionId.of(UUID.randomUUID());
    // collection dao says the collection DOES exist, which is an error in the control plane
    WorkspaceId randomWorkspaceId = WorkspaceId.of(UUID.randomUUID());
    when(collectionDao.getWorkspaceId(collectionId)).thenReturn(randomWorkspaceId);
    // sam dao says the user does have write permission
    stubWriteWorkspacePermission(randomWorkspaceId).thenReturn(true);

    // ACT/ASSERT
    // perform the import request
    assertThrows(
        CollectionException.class, () -> importService.createImport(collectionId, importRequest));
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
