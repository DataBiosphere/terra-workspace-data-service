package org.databiosphere.workspacedataservice.sam;

import org.databiosphere.workspacedataservice.retry.RestClientRetry;
import org.databiosphere.workspacedataservice.retry.RestClientRetry.RestCall;
import org.databiosphere.workspacedataservice.shared.model.WorkspaceId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Implementation of SamAuthorizationDao that accepts a SamClientFactory, then asks that factory for
 * a new ResourcesApi to use within each method invocation.
 */
public class HttpSamAuthorizationDao implements SamAuthorizationDao {

  protected final SamClientFactory samClientFactory;
  private static final Logger LOGGER = LoggerFactory.getLogger(HttpSamAuthorizationDao.class);
  private final RestClientRetry restClientRetry;
  private final WorkspaceId workspaceId;

  HttpSamAuthorizationDao(
      SamClientFactory samClientFactory, RestClientRetry restClientRetry, WorkspaceId workspaceId) {
    this.samClientFactory = samClientFactory;
    this.restClientRetry = restClientRetry;
    this.workspaceId = workspaceId;
  }

  /**
   * Check if the current user has permission to create a collection by writing to the "workspace"
   * resource in Sam. Implemented as a check for write permission on the workspace which will
   * contain this collection.
   *
   * @return true if the user has permission
   */
  @Override
  public boolean hasCreateCollectionPermission() {
    return hasPermission(ACTION_WRITE, "Sam.hasCreateCollectionPermission");
  }

  /**
   * Check if the current user has permission to delete a collection from the "workspace" resource
   * in Sam. Implemented as a check for delete permission on the resource.
   *
   * @return true if the user has permission
   */
  @Override
  public boolean hasDeleteCollectionPermission() {
    // TODO(jladieu): rather than check `ACTION_DELETE` on workspace this should check
    //  `ACTION_WRITE`; this method should cease to exist and be replaced by a method in
    //  `CollectionService` that calls `hasWriteWorkspacePermission`
    return hasPermission(ACTION_DELETE, "Sam.hasDeleteCollectionPermission");
  }

  /**
   * Check if the current user has permission to write to the "workspace" resource from Sam.
   * Implemented as a check for write permission on the resource.
   *
   * @return true if the user has permission
   */
  @Override
  public boolean hasWriteWorkspacePermission() {
    return hasPermission(ACTION_WRITE, "Sam.hasWriteWorkspacePermission");
  }

  /**
   * Check if the current user has permission to read a workspace resource from Sam.
   *
   * @return true if the user has permission
   */
  @Override
  public boolean hasReadWorkspacePermission() {
    return hasPermission(ACTION_READ, "Sam.hasReadWorkspacePermission");
  }

  /** check for permission using the configured {@link WorkspaceId} */
  private boolean hasPermission(String action, String loggerHint) {
    LOGGER.debug(
        "Checking Sam permission for {}/{}/{} ...",
        SamAuthorizationDao.RESOURCE_NAME_WORKSPACE,
        workspaceId,
        action);
    RestCall<Boolean> samFunction =
        () ->
            samClientFactory
                .getResourcesApi()
                .resourcePermissionV2(
                    SamAuthorizationDao.RESOURCE_NAME_WORKSPACE, workspaceId.toString(), action);
    return restClientRetry.withRetryAndErrorHandling(samFunction, loggerHint);
  }
}
