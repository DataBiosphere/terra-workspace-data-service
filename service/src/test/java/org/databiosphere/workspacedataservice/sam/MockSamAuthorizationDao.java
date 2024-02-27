package org.databiosphere.workspacedataservice.sam;

import org.databiosphere.workspacedataservice.shared.model.BearerToken;
import org.databiosphere.workspacedataservice.shared.model.WorkspaceId;
import org.mockito.Mockito;

/**
 * Fake implementation of {@link SamAuthorizationDao} that just returns a constant value. Intended
 * to be wrapped with a {@link Mockito#spy(Object)}. Convenience methods {@link #allowAll()} and
 * {@link #denyAll()} are provided.
 */
public class MockSamAuthorizationDao implements SamAuthorizationDao {

  private final boolean defaultReturnValue;

  /** Return a {@link SamAuthorizationDao} that allows all permissions. */
  public static SamAuthorizationDao allowAll() {
    return new MockSamAuthorizationDao(/* defaultReturnValue= */ true);
  }

  /** Return a {@link SamAuthorizationDao} that denies all permissions. */
  public static SamAuthorizationDao denyAll() {
    return new MockSamAuthorizationDao(/* defaultReturnValue= */ false);
  }

  private MockSamAuthorizationDao(boolean defaultReturnValue) {
    this.defaultReturnValue = defaultReturnValue;
  }

  @Override
  public boolean hasCreateCollectionPermission() {
    return defaultReturnValue;
  }

  @Override
  public boolean hasCreateCollectionPermission(BearerToken token) {
    return defaultReturnValue;
  }

  @Override
  public boolean hasDeleteCollectionPermission() {
    return defaultReturnValue;
  }

  @Override
  public boolean hasDeleteCollectionPermission(BearerToken token) {
    return defaultReturnValue;
  }

  @Override
  public boolean hasReadWorkspacePermission(WorkspaceId workspaceId) {
    return defaultReturnValue;
  }

  @Override
  public boolean hasReadWorkspacePermission(WorkspaceId workspaceId, BearerToken token) {
    return defaultReturnValue;
  }

  @Override
  public boolean hasWriteWorkspacePermission() {
    return defaultReturnValue;
  }

  @Override
  public boolean hasWriteWorkspacePermission(WorkspaceId workspaceId) {
    return defaultReturnValue;
  }
}
