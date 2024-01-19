package org.databiosphere.workspacedataservice.service.model;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.stream.Stream;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

class DataTypeMappingTest {
  @ParameterizedTest
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
