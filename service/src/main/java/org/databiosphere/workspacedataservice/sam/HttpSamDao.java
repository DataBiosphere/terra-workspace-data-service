package org.databiosphere.workspacedataservice.sam;

import java.util.List;
import java.util.UUID;
import org.broadinstitute.dsde.workbench.client.sam.model.SystemStatus;
import org.broadinstitute.dsde.workbench.client.sam.model.UserStatusInfo;
import org.databiosphere.workspacedataservice.model.WorkspaceId;
import org.databiosphere.workspacedataservice.retry.RestClientRetry;
import org.databiosphere.workspacedataservice.retry.RestClientRetry.RestCall;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.cache.annotation.Cacheable;

/**
 * Implementation of SamDao that accepts a SamClientFactory, then asks that factory for a new
 * ResourcesApi to use within each method invocation.
 */
public class HttpSamDao implements SamDao {

  protected final SamClientFactory samClientFactory;
  private static final Logger LOGGER = LoggerFactory.getLogger(HttpSamDao.class);
  private final RestClientRetry restClientRetry;
  private final WorkspaceId workspaceId;

  public HttpSamDao(
      SamClientFactory samClientFactory, RestClientRetry restClientRetry, String workspaceId) {
    this.samClientFactory = samClientFactory;
    this.restClientRetry = restClientRetry;
    this.workspaceId = new WorkspaceId(UUID.fromString(workspaceId));
  }

  /**
   * Check if the current user has permission to create a "wds-instance" resource in Sam.
   * Implemented as a check for write permission on the workspace which will contain this instance.
   *
   * @return true if the user has permission
   */
  @Override
  public boolean hasCreateInstancePermission() {
    return hasCreateInstancePermission(null);
  }

  @Override
  public boolean hasCreateInstancePermission(String token) {
    return hasPermission(workspaceId, ACTION_WRITE, "Sam.hasCreateInstancePermission", token);
  }

  /**
   * Check if the current user has permission to delete a "wds-instance" resource from Sam.
   * Implemented as a check for delete permission on the resource.
   *
   * @return true if the user has permission
   */
  @Override
  public boolean hasDeleteInstancePermission() {
    return hasDeleteInstancePermission(null);
  }

  @Override
  public boolean hasDeleteInstancePermission(String token) {
    return hasPermission(workspaceId, ACTION_DELETE, "Sam.hasDeleteInstancePermission", token);
  }

  // helper implementation for permission checks
  private boolean hasPermission(
      WorkspaceId workspaceId, String action, String loggerHint, String token) {
    LOGGER.debug(
        "Checking Sam permission for {}/{}/{} ...",
        SamDao.RESOURCE_NAME_WORKSPACE,
        workspaceId,
        action);
    RestCall<Boolean> samFunction =
        () ->
            samClientFactory
                .getResourcesApi(token)
                .resourcePermissionV2(
                    SamDao.RESOURCE_NAME_WORKSPACE, workspaceId.id().toString(), action);
    return restClientRetry.withRetryAndErrorHandling(samFunction, loggerHint);
  }

  /**
   * Check if the current user has permission to write to a "wds-instance" resource from Sam.
   * Implemented as a check for write permission on the resource.
   *
   * @return true if the user has permission
   */
  @Override
  public boolean hasWriteInstancePermission() {
    return hasWriteInstancePermission(null);
  }

  @Override
  public boolean hasWriteInstancePermission(String token) {
    return hasPermission(workspaceId, ACTION_WRITE, "Sam.hasWriteInstancePermission", token);
  }

  @Override
  public boolean hasReadWorkspacePermission(WorkspaceId workspaceId, String token) {
    return hasPermission(workspaceId, ACTION_READ, "Sam.hasReadWorkspacePermission", token);
  }

  @Override
  public boolean hasWriteWorkspacePermission(WorkspaceId workspaceId, String token) {
    return hasPermission(workspaceId, ACTION_WRITE, "Sam.hasWriteWorkspacePermission", token);
  }

  // this cache uses token.hashCode as its key. This prevents any logging such as
  // in CacheLogger from logging the raw token.
  @Cacheable(cacheNames = "tokenResolution", key = "#token.hashCode()")
  public String getUserId(String token) {
    return getUserInfo(token).getUserSubjectId();
  }

  public UserStatusInfo getUserInfo(String token) {
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
    RestCall<SystemStatus> samFunction = () -> samClientFactory.getStatusApi().getSystemStatus();
    return restClientRetry.withRetryAndErrorHandling(samFunction, "Sam.getSystemStatus");
  }

  public String getPetToken() {
    RestCall<String> samFunction =
        () ->
            samClientFactory
                .getGoogleApi(null)
                .getArbitraryPetServiceAccountToken(
                    List.of(
                        "https://www.googleapis.com/auth/userinfo.email",
                        "https://www.googleapis.com/auth/userinfo.profile"));
    return restClientRetry.withRetryAndErrorHandling(samFunction, "Sam.getPetToken");
  }
}
