package org.databiosphere.workspacedataservice.shared.model.attributes;

import org.databiosphere.workspacedataservice.service.model.DataTypeMapping;

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

  Object getValue();

  // TODO AJ-494: do we need this?
  DataTypeMapping getDataTypeMapping();
}
