package org.databiosphere.workspacedataservice.shared.model.attributes;

import com.fasterxml.jackson.annotation.JsonCreator;
import java.util.List;
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

  DataTypeMapping getDataTypeMapping();

  Class<?> getBaseType();

  @JsonCreator
  static Attribute create(Object input) {
    // short-circuit; if the input is already an attribute just return it
    if (input instanceof Attribute alreadyAttr) {
      return alreadyAttr;
    }
    // else, delegate creation
    if (input instanceof List<?> listInput) {
      return ArrayAttribute.create(listInput);
    } else {
      return ScalarAttribute.create(input);
    }
  }
}
