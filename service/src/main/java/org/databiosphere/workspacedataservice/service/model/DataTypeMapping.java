package org.databiosphere.workspacedataservice.service.model;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

public enum DataTypeMapping {
	NULL(String.class, "text", false, "?"),
	EMPTY_ARRAY(String[].class, "text", true, "?"),
	BOOLEAN(Boolean.class, "boolean", false, "?"),
	DATE(LocalDate.class, "date", false, "?"),
	DATE_TIME(LocalDateTime.class, "timestamp with time zone", false, "?"),
	STRING(String.class, "text", false, "?"),
	JSON(String.class, "jsonb", false, "?::jsonb"),
	NUMBER(Double.class, "numeric", false, "?"),
	ARRAY_OF_NUMBER(Double[].class, "numeric[]", true, "?::numeric[]"),
	ARRAY_OF_DATE(String[].class, "date[]", true, "?::date[]"),
	ARRAY_OF_DATE_TIME(String[].class, "timestamp with time zone[]", true, "?::timestamp with time zone[]"),
	ARRAY_OF_STRING(String[].class, "text[]", true, "?"),
	ARRAY_OF_BOOLEAN(Boolean[].class, "boolean[]", true, "?");

	private Class javaType;

	private String postgresType;

	private boolean isArrayType;

	private String writePlaceholder;

	private static final Map<String, DataTypeMapping> MAPPING_BY_PG_TYPE = new HashMap<>();

	static {
		Arrays.stream(DataTypeMapping.values()).forEach(e -> MAPPING_BY_PG_TYPE.put(e.getPostgresType(), e));
	}

	DataTypeMapping(Class javaType, String postgresType, boolean isArrayType, String writePlaceholder) {
		this.javaType = javaType;
		this.postgresType = postgresType;
		this.isArrayType = isArrayType;
		this.writePlaceholder = writePlaceholder;
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
			case NUMBER:
				return ARRAY_OF_NUMBER;
			case DATE:
				return ARRAY_OF_DATE;
			case DATE_TIME:
				return ARRAY_OF_DATE_TIME;
			default:
				throw new IllegalArgumentException("No supported array type for " + baseType);
		}
	}

	public String getWritePlaceholder() {
		return writePlaceholder;
	}
}
