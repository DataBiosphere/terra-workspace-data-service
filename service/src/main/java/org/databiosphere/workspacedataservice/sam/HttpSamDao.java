package org.databiosphere.workspacedataservice.sam;

import java.util.List;
import org.broadinstitute.dsde.workbench.client.sam.model.SystemStatus;
import org.broadinstitute.dsde.workbench.client.sam.model.UserStatusInfo;
import org.databiosphere.workspacedataservice.retry.RestClientRetry;
import org.databiosphere.workspacedataservice.retry.RestClientRetry.RestCall;
import org.databiosphere.workspacedataservice.shared.model.BearerToken;
import org.databiosphere.workspacedataservice.shared.model.WorkspaceId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.cache.annotation.Cacheable;

/**
 * Implementation of SamDao that accepts a SamClientFactory, then asks that factory for a new
 * ResourcesApi to use within each method invocation.
 */
public class HttpSamDao implements SamDao, SamAuthorizationDao {

  protected final SamClientFactory samClientFactory;
  private static final Logger LOGGER = LoggerFactory.getLogger(HttpSamDao.class);
  private final RestClientRetry restClientRetry;
  private final WorkspaceId workspaceId;

  public HttpSamDao(
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
    return hasCreateCollectionPermission(BearerToken.empty());
  }

  @Override
  public boolean hasCreateCollectionPermission(BearerToken token) {
    return hasPermission(ACTION_WRITE, "Sam.hasCreateCollectionPermission", token);
  }

  /**
   * Check if the current user has permission to delete a collection from the "workspace" resource
   * in Sam. Implemented as a check for delete permission on the resource.
   *
   * @return true if the user has permission
   */
  @Override
  public boolean hasDeleteCollectionPermission() {
    return hasDeleteCollectionPermission(BearerToken.empty());
  }

  @Override
  public boolean hasDeleteCollectionPermission(BearerToken token) {
    return hasPermission(ACTION_DELETE, "Sam.hasDeleteCollectionPermission", token);
  }

  /**
   * Check if the current user has permission to write to the "workspace" resource from Sam.
   * Implemented as a check for write permission on the resource.
   *
   * @return true if the user has permission
   */
  @Override
  public boolean hasWriteWorkspacePermission() {
    return hasWriteWorkspacePermission(workspaceId);
  }

  @Override
  public boolean hasWriteWorkspacePermission(WorkspaceId workspaceId) {
    return hasPermission(
        ACTION_WRITE, "Sam.hasWriteWorkspacePermission", workspaceId, BearerToken.empty());
  }

  /**
   * Check if the current user has permission to read a workspace resource from Sam.
   *
   * @return true if the user has permission
   */
  @Override
  public boolean hasReadWorkspacePermission(WorkspaceId workspaceId) {
    return hasReadWorkspacePermission(workspaceId, BearerToken.empty());
  }

  @Override
  public boolean hasReadWorkspacePermission(WorkspaceId workspaceId, BearerToken token) {
    return hasPermission(ACTION_READ, "Sam.hasReadWorkspacePermission", token);
  }

  // helper implementation for permission checks
  private boolean hasPermission(String action, String loggerHint, BearerToken token) {
    return hasPermission(action, loggerHint, workspaceId, token);
  }

  private boolean hasPermission(
      String action, String loggerHint, WorkspaceId workspaceId, BearerToken token) {
    LOGGER.debug(
        "Checking Sam permission for {}/{}/{} ...",
        SamAuthorizationDao.RESOURCE_NAME_WORKSPACE,
        workspaceId,
        action);
    RestCall<Boolean> samFunction =
        () ->
            samClientFactory
                .getResourcesApi(token)
                .resourcePermissionV2(
                    SamAuthorizationDao.RESOURCE_NAME_WORKSPACE, workspaceId.toString(), action);
    return restClientRetry.withRetryAndErrorHandling(samFunction, loggerHint);
  }

  // this cache uses token.hashCode as its key. This prevents any logging such as
  // in CacheLogger from logging the raw token.
  @Cacheable(cacheNames = "tokenResolution", key = "#token.hashCode()")
  public String getUserId(BearerToken token) {
    return getUserInfo(token).getUserSubjectId();
  }

  private UserStatusInfo getUserInfo(BearerToken token) {
    LOGGER.debug("Resolving Sam token to UserStatusInfo ...");
    RestCall<UserStatusInfo> samFunction =
        () -> samClientFactory.getUsersApi(token).getUserStatusInfo();
    return restClientRetry.withRetryAndErrorHandling(samFunction, "Sam.getUserInfo");
  }

  /**
   * Gets the up/down status of Sam. Using @Cacheable, will reach out to Sam no more than once every
   * 5 minutes (configured in ehcache.xml).
   */
  @Cacheable(cacheNames = "samStatus", key = "'getSystemStatus'")
  public Boolean getSystemStatusOk() {
    return getSystemStatus().getOk();
  }

  public SystemStatus getSystemStatus() {
    RestCall<SystemStatus> samFunction =
        () -> samClientFactory.getStatusApi(BearerToken.empty()).getSystemStatus();
    return restClientRetry.withRetryAndErrorHandling(samFunction, "Sam.getSystemStatus");
  }

  public String getPetToken() {
    RestCall<String> samFunction =
        () ->
            samClientFactory
                .getGoogleApi(BearerToken.empty())
                .getArbitraryPetServiceAccountToken(
                    List.of(
                        "https://www.googleapis.com/auth/userinfo.email",
                        "https://www.googleapis.com/auth/userinfo.profile"));
    return restClientRetry.withRetryAndErrorHandling(samFunction, "Sam.getPetToken");
  }
}
