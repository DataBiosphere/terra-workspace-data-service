package org.databiosphere.workspacedataservice.service.model;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

public enum DataTypeMapping {
	NULL(String.class, "text", false),
	EMPTY_ARRAY(String[].class, "text", true),
	BOOLEAN(Boolean.class, "boolean", false),
	DATE(LocalDate.class, "date", false),
	DATE_TIME(LocalDateTime.class, "timestamp with time zone", false),
	STRING(String.class, "text", false),
	JSON(String.class, "jsonb", false),
	LONG(Long.class, "bigint", false),
	DOUBLE(Double.class, "numeric", false),
	ARRAY_OF_LONG(Long[].class, "bigint[]", true),
	ARRAY_OF_STRING(String[].class, "text[]", true),
	ARRAY_OF_DOUBLE(Double[].class, "numeric[]", true),
	ARRAY_OF_BOOLEAN(Boolean[].class, "boolean[]", true);

	private Class javaType;

	private String postgresType;

	private boolean isArrayType;

	private static final Map<String, DataTypeMapping> MAPPING_BY_PG_TYPE = new HashMap<>();

	static {
		Arrays.stream(DataTypeMapping.values()).forEach(e -> MAPPING_BY_PG_TYPE.put(e.getPostgresType(), e));
	}

	DataTypeMapping(Class javaType, String postgresType, boolean isArrayType) {
		this.javaType = javaType;
		this.postgresType = postgresType;
		this.isArrayType = isArrayType;
	}

	DataTypeMapping() {
	}

	public Class getJavaType() {
		return javaType;
	}

	public String getPostgresType() {
		return postgresType;
	}

	public boolean isArrayType() {
		return isArrayType;
	}

	public static DataTypeMapping fromPostgresType(String pgType) {
		return MAPPING_BY_PG_TYPE.get(pgType);
	}

	public static DataTypeMapping getArrayTypeForBase(DataTypeMapping baseType){
		if(baseType == null){
			return EMPTY_ARRAY;
		}
		switch (baseType){
			case STRING :
				return ARRAY_OF_STRING;
			case BOOLEAN:
				return ARRAY_OF_BOOLEAN;
			case LONG:
				return ARRAY_OF_LONG;
			case DOUBLE:
				return ARRAY_OF_DOUBLE;
			default:
				throw new IllegalArgumentException("No supported array type for " + baseType);
		}
	}
}
