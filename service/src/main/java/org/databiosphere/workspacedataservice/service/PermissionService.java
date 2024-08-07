package org.databiosphere.workspacedataservice.service;

import org.databiosphere.workspacedataservice.config.TwdsProperties;
import org.databiosphere.workspacedataservice.sam.SamAuthorizationDao;
import org.databiosphere.workspacedataservice.sam.SamAuthorizationDaoFactory;
import org.databiosphere.workspacedataservice.service.model.exception.AuthenticationMaskableException;
import org.databiosphere.workspacedataservice.service.model.exception.AuthorizationException;
import org.databiosphere.workspacedataservice.shared.model.CollectionId;
import org.databiosphere.workspacedataservice.shared.model.WorkspaceId;
import org.springframework.stereotype.Service;

/** Encapsulates permission-checking duties */
@Service
public class PermissionService {

  private final SamAuthorizationDaoFactory samAuthorizationDaoFactory;
  private final CollectionService collectionService;
  private final TwdsProperties twdsProperties;

  public PermissionService(
      SamAuthorizationDaoFactory samAuthorizationDaoFactory,
      CollectionService collectionService,
      TwdsProperties twdsProperties) {
    this.samAuthorizationDaoFactory = samAuthorizationDaoFactory;
    this.collectionService = collectionService;
    this.twdsProperties = twdsProperties;
  }

  /**
   * Check if the user has write permission on the workspace. If the user does not have write
   * permission, check for read permission. Throw AuthenticationMaskableException if the user has
   * neither write nor read; throw AuthenticationException if the user does not have write but does
   * have read.
   *
   * @param workspaceId the workspace on which to check permissions
   */
  public void requireWritePermission(WorkspaceId workspaceId) {
    checkWriteAndThrow(workspaceId, "Workspace");
  }

  /**
   * Check if the user has write permission on the workspace containing a given collection;. If the
   * user does not have write permission, check for read permission. Throw
   * AuthenticationMaskableException if the user has neither write nor read; throw
   * AuthenticationException if the user does not have write but does have read.
   *
   * @param collectionId the collection on which to check permissions
   */
  public void requireWritePermission(CollectionId collectionId) {
    WorkspaceId workspaceId = collectionService.getWorkspaceId(collectionId);
    checkWriteAndThrow(workspaceId, "Collection");
  }

  // private implementation method for write permissions
  private void checkWriteAndThrow(WorkspaceId workspaceId, String objectType) {
    if (canWriteWorkspace(workspaceId)) {
      return;
    }
    if (canReadWorkspace(workspaceId)) {
      throw new AuthorizationException(
          "You are not allowed to write this %s".formatted(objectType.toLowerCase()));
    } else {
      throw new AuthenticationMaskableException(objectType);
    }
  }

  /**
   * Check if the user has read permission on the workspace; throw an error if not.
   *
   * @param workspaceId the workspace on which to check permissions
   */
  public void requireReadPermission(WorkspaceId workspaceId) {
    checkReadAndThrow(workspaceId, "Workspace");
  }

  /**
   * Check if the user has read permission on the workspace containing a given collection; throw an
   * error if not.
   *
   * @param collectionId the collection on which to check permissions
   */
  public void requireReadPermission(CollectionId collectionId) {
    WorkspaceId workspaceId = collectionService.getWorkspaceId(collectionId);
    checkReadAndThrow(workspaceId, "Collection");
  }

  // private implementation method for read permissions
  private void checkReadAndThrow(WorkspaceId workspaceId, String objectType) {
    if (canReadWorkspace(workspaceId)) {
      return;
    }
    throw new AuthenticationMaskableException(objectType);
  }

  /**
   * Check read permission on the single-tenant workspace id set in the environment.
   *
   * @deprecated Use {@link #requireReadPermission(WorkspaceId)} or {@link
   *     #requireReadPermission(CollectionId)} instead.
   */
  @Deprecated(since = "v0.14.0")
  public void requireReadPermissionSingleTenant() {
    requireReadPermission(twdsProperties.workspaceId());
  }

  /**
   * Check write permission on the single-tenant workspace id set in the environment.
   *
   * @deprecated Use {@link #requireWritePermission(WorkspaceId)} or {@link
   *     #requireWritePermission(CollectionId)} instead.
   */
  @Deprecated(since = "v0.14.0")
  public void requireWritePermissionSingleTenant() {
    requireWritePermission(twdsProperties.workspaceId());
  }

  /**
   * Check if the user has write permission on the workspace; return the result
   *
   * @param workspaceId the workspace on which to check permissions
   * @return true if the user has write permission
   */
  public boolean canWriteWorkspace(WorkspaceId workspaceId) {
    return getSamAuthorizationDao(workspaceId).hasWriteWorkspacePermission();
  }

  /**
   * Check if the user has read permission on the workspace; return the result
   *
   * @param workspaceId the workspace on which to check permissions
   * @return true if the user has read permission
   */
  public boolean canReadWorkspace(WorkspaceId workspaceId) {
    return getSamAuthorizationDao(workspaceId).hasReadWorkspacePermission();
  }

  private SamAuthorizationDao getSamAuthorizationDao(WorkspaceId workspaceId) {
    return samAuthorizationDaoFactory.getSamAuthorizationDao(workspaceId);
  }
}
