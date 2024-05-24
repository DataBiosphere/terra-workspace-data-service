package org.databiosphere.workspacedataservice.shared.model.attributes;

import org.databiosphere.workspacedataservice.service.model.DataTypeMapping;

public class FileAttribute extends ScalarAttribute<String> {
  FileAttribute(String value) {
    super(value);
  }

  @Override
  public DataTypeMapping getDataTypeMapping() {
    return DataTypeMapping.FILE;
  }
}
