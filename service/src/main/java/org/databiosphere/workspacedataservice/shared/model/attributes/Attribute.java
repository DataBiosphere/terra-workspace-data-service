package org.databiosphere.workspacedataservice.shared.model.attributes;

/**
 * Base interface for an attribute of a WDS Record. This Attribute will be part of the
 * RecordAttributes map.
 */
public interface Attribute {
  /**
   * When WDS writes this Attribute value to the db, what object should it send as a parameter to
   * the SQL statement?
   *
   * @return the SQL-compatible representation of this Attribute
   */
  Object sqlValue();
}
