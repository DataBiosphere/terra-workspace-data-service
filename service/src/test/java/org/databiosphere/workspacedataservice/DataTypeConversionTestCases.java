package org.databiosphere.workspacedataservice;

import java.math.BigDecimal;
import java.util.List;
import java.util.stream.Stream;
import org.databiosphere.workspacedataservice.service.model.DataTypeMapping;
import org.junit.jupiter.params.provider.Arguments;

public class DataTypeConversionTestCases {
  static Stream<Arguments> stringConversions() {
    return Stream.of(
        // Number
        Arguments.of(
            "123", DataTypeMapping.STRING, DataTypeMapping.NUMBER, BigDecimal.valueOf(123)),
        // Boolean
        Arguments.of("yes", DataTypeMapping.STRING, DataTypeMapping.BOOLEAN, Boolean.TRUE),
        Arguments.of("no", DataTypeMapping.STRING, DataTypeMapping.BOOLEAN, Boolean.FALSE),
        // String array
        Arguments.of(
            "foo", DataTypeMapping.STRING, DataTypeMapping.ARRAY_OF_STRING, new String[] {"foo"}),
        // Number array
        Arguments.of(
            "123",
            DataTypeMapping.STRING,
            DataTypeMapping.ARRAY_OF_NUMBER,
            new BigDecimal[] {BigDecimal.valueOf(123)}),
        // Boolean array
        Arguments.of(
            "yes",
            DataTypeMapping.STRING,
            DataTypeMapping.ARRAY_OF_BOOLEAN,
            new Boolean[] {Boolean.TRUE}));
  }

  static Stream<Arguments> stringArrayConversions() {
    return Stream.of(
        // String
        Arguments.of(
            List.of("foo"), DataTypeMapping.ARRAY_OF_STRING, DataTypeMapping.STRING, "foo"),
        // Number
        Arguments.of(
            List.of("123"),
            DataTypeMapping.ARRAY_OF_STRING,
            DataTypeMapping.NUMBER,
            BigDecimal.valueOf(123)),
        // Boolean
        Arguments.of(
            List.of("yes"), DataTypeMapping.ARRAY_OF_STRING, DataTypeMapping.BOOLEAN, Boolean.TRUE),
        // Number array
        Arguments.of(
            List.of("123"),
            DataTypeMapping.ARRAY_OF_STRING,
            DataTypeMapping.ARRAY_OF_NUMBER,
            new BigDecimal[] {BigDecimal.valueOf(123)}),
        // Boolean array
        Arguments.of(
            List.of("yes", "no"),
            DataTypeMapping.ARRAY_OF_STRING,
            DataTypeMapping.ARRAY_OF_BOOLEAN,
            new Boolean[] {Boolean.TRUE, Boolean.FALSE}));
  }

  static Stream<Arguments> numberConversions() {
    return Stream.of(
        // String
        Arguments.of(
            BigDecimal.valueOf(123), DataTypeMapping.NUMBER, DataTypeMapping.STRING, "123"),
        // Boolean
        Arguments.of(
            BigDecimal.valueOf(1), DataTypeMapping.NUMBER, DataTypeMapping.BOOLEAN, Boolean.TRUE),
        Arguments.of(
            BigDecimal.valueOf(0), DataTypeMapping.NUMBER, DataTypeMapping.BOOLEAN, Boolean.FALSE),
        // String array
        Arguments.of(
            BigDecimal.valueOf(123),
            DataTypeMapping.NUMBER,
            DataTypeMapping.ARRAY_OF_STRING,
            new String[] {"123"}),
        // Number array
        Arguments.of(
            BigDecimal.valueOf(123),
            DataTypeMapping.NUMBER,
            DataTypeMapping.ARRAY_OF_NUMBER,
            new BigDecimal[] {BigDecimal.valueOf(123)}),
        // Boolean array
        Arguments.of(
            BigDecimal.valueOf(1),
            DataTypeMapping.NUMBER,
            DataTypeMapping.ARRAY_OF_BOOLEAN,
            new Boolean[] {Boolean.TRUE}));
  }

  static Stream<Arguments> numberArrayConversions() {
    return Stream.of(
        // String
        Arguments.of(
            List.of(BigDecimal.valueOf(123)),
            DataTypeMapping.ARRAY_OF_NUMBER,
            DataTypeMapping.STRING,
            "123"),
        // Number
        Arguments.of(
            List.of(BigDecimal.valueOf(123)),
            DataTypeMapping.ARRAY_OF_NUMBER,
            DataTypeMapping.NUMBER,
            BigDecimal.valueOf(123)),
        // Boolean
        Arguments.of(
            List.of(BigDecimal.valueOf(1)),
            DataTypeMapping.ARRAY_OF_NUMBER,
            DataTypeMapping.BOOLEAN,
            Boolean.TRUE),
        // String array
        Arguments.of(
            List.of(BigDecimal.valueOf(123)),
            DataTypeMapping.ARRAY_OF_NUMBER,
            DataTypeMapping.ARRAY_OF_STRING,
            new String[] {"123"}),
        // Boolean array
        Arguments.of(
            List.of(BigDecimal.valueOf(1), BigDecimal.valueOf(0)),
            DataTypeMapping.ARRAY_OF_NUMBER,
            DataTypeMapping.ARRAY_OF_BOOLEAN,
            new Boolean[] {Boolean.TRUE, Boolean.FALSE}));
  }

  static Stream<Arguments> booleanConversions() {
    return Stream.of(
        // String
        Arguments.of(Boolean.TRUE, DataTypeMapping.BOOLEAN, DataTypeMapping.STRING, "true"),
        Arguments.of(Boolean.FALSE, DataTypeMapping.BOOLEAN, DataTypeMapping.STRING, "false"),
        // Number
        Arguments.of(
            Boolean.TRUE, DataTypeMapping.BOOLEAN, DataTypeMapping.NUMBER, BigDecimal.valueOf(1)),
        Arguments.of(
            Boolean.FALSE, DataTypeMapping.BOOLEAN, DataTypeMapping.NUMBER, BigDecimal.valueOf(0)),
        // String array
        Arguments.of(
            Boolean.TRUE,
            DataTypeMapping.BOOLEAN,
            DataTypeMapping.ARRAY_OF_STRING,
            new String[] {"true"}),
        Arguments.of(
            Boolean.FALSE,
            DataTypeMapping.BOOLEAN,
            DataTypeMapping.ARRAY_OF_STRING,
            new String[] {"false"}),
        // Number array
        Arguments.of(
            Boolean.TRUE,
            DataTypeMapping.BOOLEAN,
            DataTypeMapping.ARRAY_OF_NUMBER,
            new BigDecimal[] {BigDecimal.valueOf(1)}),
        Arguments.of(
            Boolean.FALSE,
            DataTypeMapping.BOOLEAN,
            DataTypeMapping.ARRAY_OF_NUMBER,
            new BigDecimal[] {BigDecimal.valueOf(0)}),
        // Boolean array
        Arguments.of(
            Boolean.TRUE,
            DataTypeMapping.BOOLEAN,
            DataTypeMapping.ARRAY_OF_BOOLEAN,
            new Boolean[] {Boolean.TRUE}),
        Arguments.of(
            Boolean.FALSE,
            DataTypeMapping.BOOLEAN,
            DataTypeMapping.ARRAY_OF_BOOLEAN,
            new Boolean[] {Boolean.FALSE}));
  }

  static Stream<Arguments> booleanArrayConversions() {
    return Stream.of(
        // String
        Arguments.of(
            List.of(Boolean.TRUE),
            DataTypeMapping.ARRAY_OF_BOOLEAN,
            DataTypeMapping.STRING,
            "true"),
        // Number
        Arguments.of(
            List.of(Boolean.TRUE),
            DataTypeMapping.ARRAY_OF_BOOLEAN,
            DataTypeMapping.NUMBER,
            BigDecimal.valueOf(1)),
        // Boolean
        Arguments.of(
            List.of(Boolean.TRUE),
            DataTypeMapping.ARRAY_OF_BOOLEAN,
            DataTypeMapping.BOOLEAN,
            Boolean.TRUE),
        // String array
        Arguments.of(
            List.of(Boolean.TRUE, Boolean.FALSE),
            DataTypeMapping.ARRAY_OF_BOOLEAN,
            DataTypeMapping.ARRAY_OF_STRING,
            new String[] {"true", "false"}),
        // Number array
        Arguments.of(
            List.of(Boolean.TRUE, Boolean.FALSE),
            DataTypeMapping.ARRAY_OF_BOOLEAN,
            DataTypeMapping.ARRAY_OF_NUMBER,
            new BigDecimal[] {BigDecimal.valueOf(1), BigDecimal.valueOf(0)}));
  }
}
