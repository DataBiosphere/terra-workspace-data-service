package org.databiosphere.workspacedataservice.service.model;

import java.util.Arrays;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.commons.lang3.tuple.Pair;

public enum DataTypeMapping {
  NULL(null, "text", false, "?"),
  EMPTY_ARRAY(String[].class, "text[]", true, "?"),
  BOOLEAN(null, "boolean", false, "?"),
  DATE(null, "date", false, "?"),
  DATE_TIME(null, "timestamp with time zone", false, "?"),
  STRING(null, "text", false, "?"),
  RELATION(null, "relation", false, "?"),
  JSON(null, "jsonb", false, "?::jsonb"),
  NUMBER(null, "numeric", false, "?"),
  FILE(null, "file", false, "?"),
  ARRAY_OF_NUMBER(Double[].class, "numeric[]", true, "?::numeric[]"),
  ARRAY_OF_DATE(String[].class, "date[]", true, "?::date[]"),
  ARRAY_OF_DATE_TIME(
      String[].class, "timestamp with time zone[]", true, "?::timestamp with time zone[]"),
  ARRAY_OF_STRING(String[].class, "text[]", true, "?"),
  ARRAY_OF_RELATION(String[].class, "array_of_relation", true, "?"),
  ARRAY_OF_FILE(String[].class, "array_of_file", true, "?"),
  ARRAY_OF_BOOLEAN(Boolean[].class, "boolean[]", true, "?");

  private final Class javaArrayTypeForDbWrites;

  private final String postgresType;

  private final boolean isArrayType;

  private final String writePlaceholder;

  private static final Map<String, DataTypeMapping> MAPPING_BY_PG_TYPE = new HashMap<>();

  private static final List<Pair<DataTypeMapping, DataTypeMapping>> TYPES_WITH_ARRAY_TYPES =
      List.of(
          Pair.of(STRING, ARRAY_OF_STRING),
          Pair.of(NUMBER, ARRAY_OF_NUMBER),
          Pair.of(BOOLEAN, ARRAY_OF_BOOLEAN),
          Pair.of(DATE, ARRAY_OF_DATE),
          Pair.of(DATE_TIME, ARRAY_OF_DATE_TIME),
          Pair.of(FILE, ARRAY_OF_FILE),
          Pair.of(RELATION, ARRAY_OF_RELATION));

  static {
    Arrays.stream(DataTypeMapping.values())
        .filter(v -> !EnumSet.of(EMPTY_ARRAY, NULL).contains(v))
        .forEach(e -> MAPPING_BY_PG_TYPE.put(e.getPostgresType(), e));
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
    return switch (baseType) {
      case STRING -> ARRAY_OF_STRING;
      case FILE -> ARRAY_OF_FILE;
      case RELATION -> ARRAY_OF_RELATION;
      case BOOLEAN -> ARRAY_OF_BOOLEAN;
      case NUMBER -> ARRAY_OF_NUMBER;
      case DATE -> ARRAY_OF_DATE;
      case DATE_TIME -> ARRAY_OF_DATE_TIME;
      case NULL ->
      // if we only detect nulls in the array, we can't detect the intended type.
      // treat it as a string in this case.
      ARRAY_OF_STRING;

      default -> throw new IllegalArgumentException("No supported array type for " + baseType);
    };
  }

  public DataTypeMapping getBaseType() {
    if (!isArrayType()) {
      return this;
    }

    if (this == EMPTY_ARRAY) {
      return NULL;
    }

    return TYPES_WITH_ARRAY_TYPES.stream()
        .filter(types -> types.getRight().equals(this))
        .findFirst()
        .orElseThrow()
        .getLeft();
  }

  public String getWritePlaceholder() {
    return writePlaceholder;
  }
}
