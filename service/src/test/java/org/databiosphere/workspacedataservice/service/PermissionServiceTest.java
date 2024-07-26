package org.databiosphere.workspacedataservice.service;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

import java.util.UUID;
import org.databiosphere.workspacedataservice.common.TestBase;
import org.databiosphere.workspacedataservice.sam.MockSamAuthorizationDao;
import org.databiosphere.workspacedataservice.sam.SamAuthorizationDao;
import org.databiosphere.workspacedataservice.sam.SamAuthorizationDaoFactory;
import org.databiosphere.workspacedataservice.service.model.exception.AuthenticationMaskableException;
import org.databiosphere.workspacedataservice.service.model.exception.AuthorizationException;
import org.databiosphere.workspacedataservice.service.model.exception.MissingObjectException;
import org.databiosphere.workspacedataservice.shared.model.CollectionId;
import org.databiosphere.workspacedataservice.shared.model.WorkspaceId;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.mockito.stubbing.OngoingStubbing;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.test.annotation.DirtiesContext;

@DirtiesContext
@SpringBootTest
public class PermissionServiceTest extends TestBase {
  @Autowired PermissionService permissionService;
  @MockBean SamAuthorizationDaoFactory samAuthorizationDaoFactory;
  @MockBean CollectionService collectionService;

  private final SamAuthorizationDao samAuthorizationDao = spy(MockSamAuthorizationDao.allowAll());

  // ==================== tests on WorkspaceId ====================
  @Test
  void readPermissionPassWorkspaceId() {
    WorkspaceId workspaceId = WorkspaceId.of(UUID.randomUUID());
    stubReadWorkspacePermission(workspaceId).thenReturn(true);
    permissionService.requireReadPermission(workspaceId);
  }

  @Test
  void writePermissionPassWorkspaceId() {
    WorkspaceId workspaceId = WorkspaceId.of(UUID.randomUUID());
    stubWriteWorkspacePermission(workspaceId).thenReturn(true);
    permissionService.requireWritePermission(workspaceId);
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
    permissionService.requireReadPermission(collectionId);
  }

  @Test
  void writePermissionPassCollectionId() {
    WorkspaceId workspaceId = WorkspaceId.of(UUID.randomUUID());
    CollectionId collectionId = CollectionId.of(UUID.randomUUID());
    when(collectionService.getWorkspaceId(collectionId)).thenReturn(workspaceId);

    stubWriteWorkspacePermission(workspaceId).thenReturn(true);
    permissionService.requireWritePermission(collectionId);
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

  @Disabled
  @Test
  void mismatchedWorkspaceId() {}

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
