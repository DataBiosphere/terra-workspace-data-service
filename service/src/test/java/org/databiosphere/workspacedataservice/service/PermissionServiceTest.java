package org.databiosphere.workspacedataservice.service;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

import java.util.UUID;
import org.databiosphere.workspacedataservice.common.ControlPlaneTestBase;
import org.databiosphere.workspacedataservice.sam.MockSamAuthorizationDao;
import org.databiosphere.workspacedataservice.sam.SamAuthorizationDao;
import org.databiosphere.workspacedataservice.sam.SamAuthorizationDaoFactory;
import org.databiosphere.workspacedataservice.service.model.exception.AuthenticationMaskableException;
import org.databiosphere.workspacedataservice.service.model.exception.AuthorizationException;
import org.databiosphere.workspacedataservice.service.model.exception.CollectionException;
import org.databiosphere.workspacedataservice.service.model.exception.MissingObjectException;
import org.databiosphere.workspacedataservice.service.model.exception.RestException;
import org.databiosphere.workspacedataservice.shared.model.CollectionId;
import org.databiosphere.workspacedataservice.shared.model.WorkspaceId;
import org.junit.jupiter.api.Test;
import org.mockito.stubbing.OngoingStubbing;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.http.HttpStatus;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.bean.override.mockito.MockitoBean;

@DirtiesContext
@SpringBootTest
class PermissionServiceTest extends ControlPlaneTestBase {
  @Autowired PermissionService permissionService;
  @MockitoBean SamAuthorizationDaoFactory samAuthorizationDaoFactory;
  @MockitoBean CollectionService collectionService;

  private final SamAuthorizationDao samAuthorizationDao = spy(MockSamAuthorizationDao.allowAll());

  // ==================== tests on WorkspaceId ====================
  @Test
  void readPermissionPassWorkspaceId() {
    WorkspaceId workspaceId = WorkspaceId.of(UUID.randomUUID());
    stubReadWorkspacePermission(workspaceId).thenReturn(true);
    assertDoesNotThrow(() -> permissionService.requireReadPermission(workspaceId));
  }

  @Test
  void writePermissionPassWorkspaceId() {
    WorkspaceId workspaceId = WorkspaceId.of(UUID.randomUUID());
    stubWriteWorkspacePermission(workspaceId).thenReturn(true);
    assertDoesNotThrow(() -> permissionService.requireWritePermission(workspaceId));
  }

  @Test
  void noReadPermissionThrowWorkspaceId() {
    WorkspaceId workspaceId = WorkspaceId.of(UUID.randomUUID());
    stubReadWorkspacePermission(workspaceId).thenReturn(false);
    AuthenticationMaskableException actual =
        assertThrows(
            AuthenticationMaskableException.class,
            () -> permissionService.requireReadPermission(workspaceId));
    assertEquals("Workspace", actual.getObjectType());
  }

  @Test
  void noWriteYesReadPermissionThrowWorkspaceId() {
    WorkspaceId workspaceId = WorkspaceId.of(UUID.randomUUID());
    stubReadWorkspacePermission(workspaceId).thenReturn(true);
    stubWriteWorkspacePermission(workspaceId).thenReturn(false);
    AuthorizationException actual =
        assertThrows(
            AuthorizationException.class,
            () -> permissionService.requireWritePermission(workspaceId));
    assertThat(actual.getMessage()).endsWith("workspace\"");
  }

  @Test
  void noWriteNoReadPermissionThrowWorkspaceId() {
    WorkspaceId workspaceId = WorkspaceId.of(UUID.randomUUID());
    stubReadWorkspacePermission(workspaceId).thenReturn(false);
    stubWriteWorkspacePermission(workspaceId).thenReturn(false);
    AuthenticationMaskableException actual =
        assertThrows(
            AuthenticationMaskableException.class,
            () -> permissionService.requireWritePermission(workspaceId));
    assertEquals("Workspace", actual.getObjectType());
  }

  // ==================== tests on CollectionId ====================
  @Test
  void readPermissionPassCollectionId() {
    WorkspaceId workspaceId = WorkspaceId.of(UUID.randomUUID());
    CollectionId collectionId = CollectionId.of(UUID.randomUUID());
    when(collectionService.getWorkspaceId(collectionId)).thenReturn(workspaceId);

    stubReadWorkspacePermission(workspaceId).thenReturn(true);
    assertDoesNotThrow(() -> permissionService.requireReadPermission(collectionId));
  }

  @Test
  void writePermissionPassCollectionId() {
    WorkspaceId workspaceId = WorkspaceId.of(UUID.randomUUID());
    CollectionId collectionId = CollectionId.of(UUID.randomUUID());
    when(collectionService.getWorkspaceId(collectionId)).thenReturn(workspaceId);

    stubWriteWorkspacePermission(workspaceId).thenReturn(true);
    assertDoesNotThrow(() -> permissionService.requireWritePermission(collectionId));
  }

  @Test
  void noReadPermissionThrowCollectionId() {
    WorkspaceId workspaceId = WorkspaceId.of(UUID.randomUUID());
    CollectionId collectionId = CollectionId.of(UUID.randomUUID());
    when(collectionService.getWorkspaceId(collectionId)).thenReturn(workspaceId);

    stubReadWorkspacePermission(workspaceId).thenReturn(false);
    AuthenticationMaskableException actual =
        assertThrows(
            AuthenticationMaskableException.class,
            () -> permissionService.requireReadPermission(collectionId));
    assertEquals("Collection", actual.getObjectType());
  }

  @Test
  void noWriteYesReadPermissionThrowCollectionId() {
    WorkspaceId workspaceId = WorkspaceId.of(UUID.randomUUID());
    CollectionId collectionId = CollectionId.of(UUID.randomUUID());
    when(collectionService.getWorkspaceId(collectionId)).thenReturn(workspaceId);

    stubReadWorkspacePermission(workspaceId).thenReturn(true);
    stubWriteWorkspacePermission(workspaceId).thenReturn(false);
    AuthorizationException actual =
        assertThrows(
            AuthorizationException.class,
            () -> permissionService.requireWritePermission(collectionId));
    assertThat(actual.getMessage()).endsWith("collection\"");
  }

  @Test
  void noWriteNoReadPermissionThrowCollectionId() {
    WorkspaceId workspaceId = WorkspaceId.of(UUID.randomUUID());
    CollectionId collectionId = CollectionId.of(UUID.randomUUID());
    when(collectionService.getWorkspaceId(collectionId)).thenReturn(workspaceId);

    stubReadWorkspacePermission(workspaceId).thenReturn(false);
    stubWriteWorkspacePermission(workspaceId).thenReturn(false);
    AuthenticationMaskableException actual =
        assertThrows(
            AuthenticationMaskableException.class,
            () -> permissionService.requireWritePermission(collectionId));
    assertEquals("Collection", actual.getObjectType());
  }

  // ==================== tests for corner cases on CollectionId ====================

  @Test
  void collectionNotFound() {
    WorkspaceId workspaceId = WorkspaceId.of(UUID.randomUUID());
    CollectionId collectionId = CollectionId.of(UUID.randomUUID());
    when(collectionService.getWorkspaceId(collectionId))
        .thenThrow(new MissingObjectException("Collection"));

    stubWriteWorkspacePermission(workspaceId).thenReturn(true);

    MissingObjectException actual =
        assertThrows(
            MissingObjectException.class,
            () -> permissionService.requireWritePermission(collectionId));
    assertThat(actual.getMessage()).startsWith("Collection");
  }

  @Test
  void mismatchedWorkspaceId() {
    WorkspaceId workspaceId = WorkspaceId.of(UUID.randomUUID());
    CollectionId collectionId = CollectionId.of(UUID.randomUUID());
    when(collectionService.getWorkspaceId(collectionId)).thenReturn(workspaceId);

    stubWriteWorkspacePermission(workspaceId)
        .thenThrow(
            new CollectionException(
                "Found unexpected workspaceId for collection %s.".formatted(collectionId)));

    CollectionException actual =
        assertThrows(
            CollectionException.class,
            () -> permissionService.requireWritePermission(collectionId));
    assertEquals(
        "Found unexpected workspaceId for collection %s.".formatted(collectionId),
        actual.getMessage());
  }

  // what if the call to Sam throws a connection error or other unexpected error?
  @Test
  void unexpectedSamErrorRead() {
    WorkspaceId workspaceId = WorkspaceId.of(UUID.randomUUID());
    CollectionId collectionId = CollectionId.of(UUID.randomUUID());
    when(collectionService.getWorkspaceId(collectionId)).thenReturn(workspaceId);

    // unexpected ApiExceptions are wrapped in RestException by RestClientRetry
    stubReadWorkspacePermission(workspaceId)
        .thenThrow(new RestException(HttpStatus.I_AM_A_TEAPOT, "unit test intentional error"));

    // the RestException is bubbled up
    RestException actual =
        assertThrows(
            RestException.class, () -> permissionService.requireReadPermission(collectionId));
    assertEquals(HttpStatus.I_AM_A_TEAPOT, actual.getStatusCode());
    assertThat(actual.getMessage()).contains("unit test intentional error");
  }

  @Test
  void unexpectedSamErrorWrite() {
    WorkspaceId workspaceId = WorkspaceId.of(UUID.randomUUID());
    CollectionId collectionId = CollectionId.of(UUID.randomUUID());
    when(collectionService.getWorkspaceId(collectionId)).thenReturn(workspaceId);

    // unexpected ApiExceptions are wrapped in RestException by RestClientRetry
    stubWriteWorkspacePermission(workspaceId)
        .thenThrow(new RestException(HttpStatus.I_AM_A_TEAPOT, "unit test intentional error"));

    // the RestException is bubbled up
    RestException actual =
        assertThrows(
            RestException.class, () -> permissionService.requireWritePermission(collectionId));
    assertEquals(HttpStatus.I_AM_A_TEAPOT, actual.getStatusCode());
    assertThat(actual.getMessage()).contains("unit test intentional error");
  }

  private OngoingStubbing<Boolean> stubReadWorkspacePermission(WorkspaceId workspaceId) {
    when(samAuthorizationDaoFactory.getSamAuthorizationDao(workspaceId))
        .thenReturn(samAuthorizationDao);
    return when(samAuthorizationDao.hasReadWorkspacePermission());
  }

  private OngoingStubbing<Boolean> stubWriteWorkspacePermission(WorkspaceId workspaceId) {
    when(samAuthorizationDaoFactory.getSamAuthorizationDao(workspaceId))
        .thenReturn(samAuthorizationDao);
    return when(samAuthorizationDao.hasWriteWorkspacePermission());
  }
}
