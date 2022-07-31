package org.databiosphere.workspacedataservice.dao;

public enum EntitySystemColumn {
    ENTITY_ID("sys_name");
    private final String columnName;

    EntitySystemColumn(String columnName){
        this.columnName = columnName;
    }

    public String getColumnName() {
        return columnName;
    }
}
