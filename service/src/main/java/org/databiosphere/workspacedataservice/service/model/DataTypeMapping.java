package org.databiosphere.workspacedataservice.service.model;

import com.google.common.collect.ImmutableList;
import java.util.Arrays;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.Map;
import java.util.NoSuchElementException;

public enum DataTypeMapping {
  NULL(null, "text", false, "?"),
  EMPTY_ARRAY(String[].class, "text[]", true, "?"),
  BOOLEAN(null, "boolean", false, "?"),
  DATE(null, "date", false, "?"),
  DATE_TIME(null, "timestamp with time zone", false, "?"),
  STRING(null, "text", false, "?"),
  RELATION(null, "public.relation", false, "?"),
  JSON(null, "jsonb", false, "?::jsonb"),
  NUMBER(null, "numeric", false, "?"),
  FILE(null, "public.file", false, "?"),
  ARRAY_OF_NUMBER(Double[].class, "numeric[]", true, "?::numeric[]"),
  ARRAY_OF_DATE(String[].class, "date[]", true, "?::date[]"),
  ARRAY_OF_DATE_TIME(
      String[].class, "timestamp with time zone[]", true, "?::timestamp with time zone[]"),
  ARRAY_OF_STRING(String[].class, "text[]", true, "?"),
  ARRAY_OF_RELATION(String[].class, "public.array_of_relation", true, "?"),
  ARRAY_OF_FILE(String[].class, "public.array_of_file", true, "?"),
  ARRAY_OF_BOOLEAN(Boolean[].class, "boolean[]", true, "?"),
  ARRAY_OF_JSON(String[].class, "jsonb[]", true, "?::jsonb[]");

  private final Class javaArrayTypeForDbWrites;

  private final String postgresType;

  private final boolean isArrayType;

  private final String writePlaceholder;

  private static final Map<String, DataTypeMapping> MAPPING_BY_PG_TYPE = new HashMap<>();

  private record BaseTypeAndArrayTypePair(DataTypeMapping baseType, DataTypeMapping arrayType) {}

  private static final ImmutableList<BaseTypeAndArrayTypePair> BASE_TYPE_AND_ARRAY_TYPE_PAIRS =
      ImmutableList.of(
          new BaseTypeAndArrayTypePair(STRING, ARRAY_OF_STRING),
          new BaseTypeAndArrayTypePair(NUMBER, ARRAY_OF_NUMBER),
          new BaseTypeAndArrayTypePair(BOOLEAN, ARRAY_OF_BOOLEAN),
          new BaseTypeAndArrayTypePair(DATE, ARRAY_OF_DATE),
          new BaseTypeAndArrayTypePair(DATE_TIME, ARRAY_OF_DATE_TIME),
          new BaseTypeAndArrayTypePair(FILE, ARRAY_OF_FILE),
          new BaseTypeAndArrayTypePair(RELATION, ARRAY_OF_RELATION),
          new BaseTypeAndArrayTypePair(JSON, ARRAY_OF_JSON));

  // when building the MAPPING_BY_PG_TYPE, strip the "public." qualifier from postgres types.
  // we need that qualifier when generating SQL, but the qualifier will not be returned by
  // postgres when describing columns.
  static {
    Arrays.stream(DataTypeMapping.values())
        .filter(v -> !EnumSet.of(EMPTY_ARRAY, NULL).contains(v))
        .forEach(e -> MAPPING_BY_PG_TYPE.put(e.getPostgresType().replace("public.", ""), e));

    // TODO: extra mappings to handle type translations for aggregate functions in views
    MAPPING_BY_PG_TYPE.put("bigint", NUMBER);
  }

  DataTypeMapping(
      Class javaType, String postgresType, boolean isArrayType, String writePlaceholder) {
    this.javaArrayTypeForDbWrites = javaType;
    this.postgresType = postgresType;
    this.isArrayType = isArrayType;
    this.writePlaceholder = writePlaceholder;
  }

  public Class getJavaArrayTypeForDbWrites() {
    return javaArrayTypeForDbWrites;
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

  public static DataTypeMapping getArrayTypeForBase(DataTypeMapping baseType) {
    if (baseType == null) {
      return EMPTY_ARRAY;
    }

    // if we only detect nulls in the array, we can't detect the intended type.
    // treat it as a string in this case.
    if (baseType == NULL) {
      return ARRAY_OF_STRING;
    }

    try {
      return BASE_TYPE_AND_ARRAY_TYPE_PAIRS.stream()
          .filter(types -> types.baseType().equals(baseType))
          .findFirst()
          .orElseThrow()
          .arrayType();
    } catch (NoSuchElementException e) {
      throw new IllegalArgumentException("No supported array type for " + baseType);
    }
  }

  public DataTypeMapping getBaseType() {
    if (!isArrayType()) {
      return this;
    }

    if (this == EMPTY_ARRAY) {
      return NULL;
    }

    return BASE_TYPE_AND_ARRAY_TYPE_PAIRS.stream()
        .filter(types -> types.arrayType().equals(this))
        .findFirst()
        .orElseThrow()
        .baseType();
  }

  public String getWritePlaceholder() {
    return writePlaceholder;
  }
}
