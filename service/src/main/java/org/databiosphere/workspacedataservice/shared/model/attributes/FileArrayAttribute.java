package org.databiosphere.workspacedataservice.shared.model.attributes;

import java.util.List;
import org.databiosphere.workspacedataservice.service.model.DataTypeMapping;

public class FileArrayAttribute extends ArrayAttribute<FileAttribute> {
  FileArrayAttribute(List<FileAttribute> value) {
    super(value);
  }

  @Override
  public DataTypeMapping getDataTypeMapping() {
    return DataTypeMapping.ARRAY_OF_FILE;
  }

  @Override
  public String[] sqlValue() {
    return this.value.stream().map(FileAttribute::sqlValue).toArray(String[]::new);
  }

  @Override
  public List<String> getValue() {
    return this.value.stream().map(FileAttribute::getValue).toList();
  }
}
