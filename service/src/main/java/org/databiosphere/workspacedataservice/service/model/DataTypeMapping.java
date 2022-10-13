package org.databiosphere.workspacedataservice.service.model;

import java.sql.Types;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

public enum DataTypeMapping {
	NULL(String.class, "text", false),
	BOOLEAN(Boolean.class, "boolean", false),
	DATE(LocalDate.class, "date", false),
	DATE_TIME(LocalDateTime.class, "timestamp with time zone", false),
	STRING(String.class, "text", false),
	JSON(String.class, "jsonb", false),
	LONG(Long.class, "bigint", false),
	DOUBLE(Double.class, "numeric", false),
	ARRAY_OF_LONG(Long[].class, "bigint[]", true),
	ARRAY_OF_TEXT(String[].class, "text[]", true),
	ARRAY_OF_DOUBLE(Double[].class, "numeric[]", true),
	ARRAY_OF_BOOLEAN(Boolean[].class, "boolean[]", true),
	ARRAY_OF_DATE_TIME(LocalDateTime[].class, "timestamp with time zone[]", true),
	ARRAY_OF_DATE(LocalDate[].class, "date[]", true);

	private Class javaType;

	private String postgresType;

	private boolean isArrayType;

	private static final Map<String, DataTypeMapping> MAPPING_BY_PG_TYPE = new HashMap<>();

	static {
		Arrays.stream(DataTypeMapping.values()).forEach(e -> MAPPING_BY_PG_TYPE.put(e.getPostgresType(), e));
	}

	private int sqlTypeInt;

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
}
