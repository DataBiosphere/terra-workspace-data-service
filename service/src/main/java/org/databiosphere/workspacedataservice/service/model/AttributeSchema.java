package org.databiosphere.workspacedataservice.service.model;

import com.fasterxml.jackson.annotation.JsonInclude;
import org.databiosphere.workspacedataservice.shared.model.RecordType;

@JsonInclude(JsonInclude.Include.NON_NULL)
public record AttributeSchema(String name, String datatype, RecordType relatesTo) {

  public AttributeSchema(String name, DataTypeMapping datatype, RecordType relatesTo) {
    this(name, datatype.name(), relatesTo);
  }

  public AttributeSchema(String name, String datatype) {
    this(name, datatype, null);
  }

  public AttributeSchema(String name, DataTypeMapping datatype) {
    this(name, datatype, null);
  }

  public AttributeSchema(String name) {
    this(name, (String) null, null);
  }
}
