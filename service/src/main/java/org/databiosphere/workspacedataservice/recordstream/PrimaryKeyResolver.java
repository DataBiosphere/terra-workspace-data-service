package org.databiosphere.workspacedataservice.recordstream;

/**
 * Simple interface to clarify the additional responsibility that {@link TsvStreamWriteHandler}
 * provides.
 */
public interface PrimaryKeyResolver {
  String getPrimaryKey();
}
