package org.databiosphere.workspacedataservice.service.model;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.HashMap;
import java.util.Map;

public enum DataTypeMapping {
    DATE(LocalDate.class, "date"), DATE_TIME(LocalDateTime.class, "timestamptz"),
    STRING(String.class, "text"), JSON(String.class, "jsonb"), LONG(Long.class, "bigint"),
    DOUBLE(Double.class, "numeric"), FOR_ATTRIBUTE_DEL();

    private Class javaType;

    private String postgresType;

    private static final Map<String, DataTypeMapping> MAPPING_BY_PG_TYPE = new HashMap<>();

    static {
        MAPPING_BY_PG_TYPE.put("date", DATE);
        MAPPING_BY_PG_TYPE.put("timestamp with time zone", DATE_TIME);
        MAPPING_BY_PG_TYPE.put("text", STRING);
        MAPPING_BY_PG_TYPE.put("jsonb", JSON);
        MAPPING_BY_PG_TYPE.put("bigint", LONG);
        MAPPING_BY_PG_TYPE.put("numeric", DOUBLE);
    }

    DataTypeMapping(Class javaType, String postgresType) {
        this.javaType = javaType;
        this.postgresType = postgresType;
    }

    DataTypeMapping() {
    }

    public Class getJavaType() {
        return javaType;
    }

    public String getPostgresType() {
        return postgresType;
    }

    public static DataTypeMapping fromPostgresType(String pgType){
        return MAPPING_BY_PG_TYPE.get(pgType);
    }
}
