package org.databiosphere.workspacedataservice.shared.model;

import org.databiosphere.workspacedataservice.service.model.DataTypeMapping;

public record RecordColumn(String colName, DataTypeMapping typeMapping) {

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (!(o instanceof RecordColumn that)) return false;

    if (!colName.equals(that.colName)) return false;
    return typeMapping == that.typeMapping;
  }

  @Override
  public int hashCode() {
    int result = colName.hashCode();
    result = 31 * result + typeMapping.hashCode();
    return result;
  }
}
