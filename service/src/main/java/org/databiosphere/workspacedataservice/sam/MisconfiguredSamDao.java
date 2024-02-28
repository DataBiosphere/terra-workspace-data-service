package org.databiosphere.workspacedataservice.sam;

import org.broadinstitute.dsde.workbench.client.sam.model.SystemStatus;
import org.databiosphere.workspacedataservice.shared.model.BearerToken;
import org.databiosphere.workspacedataservice.shared.model.WorkspaceId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class will be used instead of HttpSamDao if the WORKSPACE_ID env var contains a non-UUID
 * value, including being blank.
 *
 * <p>This class will fail all permission checks and log a warning each time. Its objective is to
 * disallow any write operations that require permissions, while still allowing WDS to start up and
 * for users to read any data already in WDS.
 */
public class MisconfiguredSamDao implements SamDao {

  private final String errorMessage;

  private final Logger logger = LoggerFactory.getLogger(getClass());

  public MisconfiguredSamDao(String errorMessage) {
    this.errorMessage = errorMessage;
  }

  @Override
  public boolean hasCreateCollectionPermission() {
    logWarning();
    return false;
  }

  @Override
  public boolean hasCreateCollectionPermission(BearerToken token) {
    return hasCreateCollectionPermission();
  }

  @Override
  public boolean hasDeleteCollectionPermission() {
    logWarning();
    return false;
  }

  @Override
  public boolean hasDeleteCollectionPermission(BearerToken token) {
    return hasDeleteCollectionPermission();
  }

  @Override
  public boolean hasWriteWorkspacePermission() {
    logWarning();
    return false;
  }

  @Override
  public boolean hasWriteWorkspacePermission(WorkspaceId workspaceId) {
    return hasWriteWorkspacePermission();
  }

  @Override
  public boolean hasReadWorkspacePermission(WorkspaceId workspaceId) {
    logWarning();
    return false;
  }

  @Override
  public boolean hasReadWorkspacePermission(WorkspaceId workspaceId, BearerToken token) {
    return hasReadWorkspacePermission(workspaceId);
  }

  @Override
  public String getUserEmail(BearerToken token) {
    logWarning();
    return "n/a";
  }

  @Override
  public String getUserId(BearerToken token) {
    logWarning();
    return "n/a";
  }

  @Override
  public Boolean getSystemStatusOk() {
    throw new RuntimeException("Sam integration failure: " + errorMessage);
  }

  @Override
  public SystemStatus getSystemStatus() {
    throw new RuntimeException("Sam integration failure: " + errorMessage);
  }

  @Override
  public String getPetToken() {
    logWarning();
    return "n/a";
  }

  private void logWarning() {
    logger.warn("Sam permission check failed. {}", errorMessage);
  }
}
