package org.databiosphere.workspacedataservice.tsv;

import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.ArgumentsProvider;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.Arrays;
import java.util.Map;
import java.util.stream.Stream;

/**
 * Test fixtures for use by TsvDeserializerTest (and only TsvDeserializerTest).
 * <p>
 * The fixtures in here cause JSON parsing errors but are allowed for TSVs.
 *
 * @see TsvDeserializerTest
 */
public class TsvOnlyArgumentsProvider implements ArgumentsProvider {

    @Override
    public Stream<? extends Arguments> provideArguments(ExtensionContext context) {
        /* Arguments are sets:
			- first value is the text that would be contained in a TSV cell
			- second value is the expected Java type that the TsvConverter would create for that cell
		*/
        return Stream.of(
                // alternate casings for booleans
                Arguments.of("True", true),
                Arguments.of("TRUE", true),
                Arguments.of("tRuE", true),
                Arguments.of("False", false),
                Arguments.of("FALSE", false),
                Arguments.of("fAlSe", false),
                Arguments.of("[tRuE, fAlSe, TRUE, FALSE]", Arrays.asList(true, false, true, false)),

                // quotes and smart-quotes
                Arguments.of("\"Hello world\"", "Hello world"), // quoted string inside TSV
                Arguments.of("“Hello world”", "“Hello world”"), // smart quotes around a scalar: keep the smart quotes
                Arguments.of("\"“Hello world”\"", "“Hello world”"), // smart quotes inside a quoted string: keep the smart quotes
                Arguments.of("[“test1”, “test2”]", Arrays.asList("test1", "test2")), // smart quotes inside an array: strip quotes

                // null and empty string
                Arguments.of("", null),
                Arguments.of("\"\"", ""),

                // embedded newlines and tabs
                Arguments.of("Embedded\tTab", "Embedded\tTab"),
                Arguments.of("\n,Weird\n String", "\n,Weird\n String"),

                // leading zeros on numbers
                // these could be enabled in JSON by turning on JsonReadFeature.ALLOW_LEADING_ZEROS_FOR_NUMBERS in the mapper
                Arguments.of("00005", BigInteger.valueOf(5)), // leading zeros
                Arguments.of("003.45", BigDecimal.valueOf(3.45)), // leading zeros

                // leading decimal point on numbers
                // these could be enabled in JSON by turning on JsonReadFeature.ALLOW_LEADING_DECIMAL_POINT_FOR_NUMBERS in the mapper
                Arguments.of(".123", BigDecimal.valueOf(0.123)) // leading decimal
        );
    }
}
