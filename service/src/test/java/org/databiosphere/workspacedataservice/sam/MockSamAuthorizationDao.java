package org.databiosphere.workspacedataservice.sam;

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
  public boolean hasReadWorkspacePermission() {
    return defaultReturnValue;
  }

  @Override
  public boolean hasWriteWorkspacePermission() {
    return defaultReturnValue;
  }
}
