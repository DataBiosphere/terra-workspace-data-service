package org.databiosphere.workspacedataservice.recordstream;

/**
 * Simple interface to clarify the additional responsibility that {@link TsvRecordSource} provides.
 * This interface is not strictly necessary, but I wanted it to be a clear marker of where the
 * additional responsibility is used to make it easier to refactor and tidy up later.
 */
public interface PrimaryKeyResolver {
  String getPrimaryKey();
}
