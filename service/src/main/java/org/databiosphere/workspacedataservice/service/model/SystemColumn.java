package org.databiosphere.workspacedataservice.service.model;

public enum SystemColumn {
  RECORD_ID("sys_name");
  private final String columnName;

  SystemColumn(String columnName) {
    this.columnName = columnName;
  }

  public String getColumnName() {
    return columnName;
  }
}
