package org.databiosphere.workspacedataservice.service.model;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

class DataTypeMappingTest {
  @ParameterizedTest(name = "getArrayTypeForBase maps {0} to {1}")
  @MethodSource("getBaseTypesAndArrayTypes")
  void testGetArrayTypeForBase(DataTypeMapping baseType, DataTypeMapping expectedArrayType) {
    DataTypeMapping arrayType = DataTypeMapping.getArrayTypeForBase(baseType);
    assertEquals(expectedArrayType, arrayType);
  }

  private static Stream<Arguments> getBaseTypesAndArrayTypes() {
    return Stream.of(
        Arguments.of(null, DataTypeMapping.EMPTY_ARRAY),
        Arguments.of(DataTypeMapping.STRING, DataTypeMapping.ARRAY_OF_STRING),
        Arguments.of(DataTypeMapping.FILE, DataTypeMapping.ARRAY_OF_FILE),
        Arguments.of(DataTypeMapping.RELATION, DataTypeMapping.ARRAY_OF_RELATION),
        Arguments.of(DataTypeMapping.BOOLEAN, DataTypeMapping.ARRAY_OF_BOOLEAN),
        Arguments.of(DataTypeMapping.NUMBER, DataTypeMapping.ARRAY_OF_NUMBER),
        Arguments.of(DataTypeMapping.DATE, DataTypeMapping.ARRAY_OF_DATE),
        Arguments.of(DataTypeMapping.DATE_TIME, DataTypeMapping.ARRAY_OF_DATE_TIME),
        Arguments.of(DataTypeMapping.JSON, DataTypeMapping.JSON),
        Arguments.of(DataTypeMapping.NULL, DataTypeMapping.ARRAY_OF_STRING));
  }

  @Test
  void testGetArrayTypeForBaseNoArrayType() {
    IllegalArgumentException e =
        assertThrows(
            IllegalArgumentException.class,
            () -> DataTypeMapping.getArrayTypeForBase(DataTypeMapping.EMPTY_ARRAY),
            "getArrayTypeForBase should have thrown an error");
    assertEquals("No supported array type for EMPTY_ARRAY", e.getMessage());
  }

  @ParameterizedTest(name = "getBaseType maps {0} to {1}")
  @MethodSource("getTypesAndBaseTypes")
  void testGetBaseType(DataTypeMapping type, DataTypeMapping expectedBaseType) {
    DataTypeMapping baseType = type.getBaseType();
    assertEquals(expectedBaseType, baseType);
  }

  private static Stream<Arguments> getTypesAndBaseTypes() {
    return Stream.of(
        Arguments.of(DataTypeMapping.NULL, DataTypeMapping.NULL),
        Arguments.of(DataTypeMapping.STRING, DataTypeMapping.STRING),
        Arguments.of(DataTypeMapping.FILE, DataTypeMapping.FILE),
        Arguments.of(DataTypeMapping.RELATION, DataTypeMapping.RELATION),
        Arguments.of(DataTypeMapping.BOOLEAN, DataTypeMapping.BOOLEAN),
        Arguments.of(DataTypeMapping.NUMBER, DataTypeMapping.NUMBER),
        Arguments.of(DataTypeMapping.DATE, DataTypeMapping.DATE),
        Arguments.of(DataTypeMapping.DATE_TIME, DataTypeMapping.DATE_TIME),
        Arguments.of(DataTypeMapping.JSON, DataTypeMapping.JSON),
        Arguments.of(DataTypeMapping.EMPTY_ARRAY, DataTypeMapping.NULL),
        Arguments.of(DataTypeMapping.ARRAY_OF_STRING, DataTypeMapping.STRING),
        Arguments.of(DataTypeMapping.ARRAY_OF_FILE, DataTypeMapping.FILE),
        Arguments.of(DataTypeMapping.ARRAY_OF_RELATION, DataTypeMapping.RELATION),
        Arguments.of(DataTypeMapping.ARRAY_OF_BOOLEAN, DataTypeMapping.BOOLEAN),
        Arguments.of(DataTypeMapping.ARRAY_OF_NUMBER, DataTypeMapping.NUMBER),
        Arguments.of(DataTypeMapping.ARRAY_OF_DATE, DataTypeMapping.DATE),
        Arguments.of(DataTypeMapping.ARRAY_OF_DATE_TIME, DataTypeMapping.DATE_TIME));
  }
}
