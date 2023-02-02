package org.databiosphere.workspacedataservice.tsv;

import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.ArgumentsProvider;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.AbstractMap;
import java.util.Arrays;
import java.util.List;
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
    public Stream<? extends Arguments> provideArguments(ExtensionContext context) throws Exception {
        /* Arguments are sets:
			- first value is the text that would be contained in a TSV cell
			- second value is the expected Java type that the TsvConverter would create for that cell
		*/
        return Stream.of(
                Arguments.of("True", true),
                Arguments.of("TRUE", true),
                Arguments.of("tRuE", true),
                Arguments.of("False", false),
                Arguments.of("FALSE", false),
                Arguments.of("fAlSe", false),
                Arguments.of("\"Hello world\"", "Hello world"), // quoted string inside TSV
                Arguments.of("“Hello world”", "“Hello world”"), // smart quotes around a scalar: keep the smart quotes
                Arguments.of("\"“Hello world”\"", "“Hello world”"), // smart quotes inside a quoted string: keep the smart quotes
                Arguments.of("[“test1”, “test2”]", Arrays.asList("test1", "test2")), // smart quotes inside an array: strip quotes
                Arguments.of("[tRuE, fAlSe, TRUE, FALSE]", Arrays.asList(true, false, true, false))
                // Arguments.of("[\"True\", \"false\", \"true   \"]", Arrays.asList(true, false, true)) // quoted booleans inside an array
        );
    }
}
