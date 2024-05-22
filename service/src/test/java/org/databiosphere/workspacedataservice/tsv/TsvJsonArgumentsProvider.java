package org.databiosphere.workspacedataservice.tsv;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.Collections;
import java.util.List;
import java.util.stream.Stream;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.ArgumentsProvider;

/**
 * Test fixtures for use by TsvDeserializerTest and TsvJsonEquivalenceTest.
 *
 * <p>The fixtures define both JSON and TSV input values and the objects we expect to see after
 * deserializing those inputs.
 *
 * @see TsvDeserializerTest
 * @see TsvJsonEquivalenceTest
 */
public class TsvJsonArgumentsProvider implements ArgumentsProvider {

  @Override
  public Stream<? extends Arguments> provideArguments(ExtensionContext context) {
    /* Arguments are sets:
    	- first value is the text that would be contained in a TSV cell or JSON value
    	- second value is the expected Java type that the TsvConverter or JSON deserializer would create for that cell
    	- third value, a boolean, indicates whether the value should be quoted inside a JSON packet
    */
    return Stream.of(
        // ========== scalars ==========

        // booleans
        Arguments.of("true", true, false),
        Arguments.of("false", false, false),

        // integers
        Arguments.of("1", BigInteger.valueOf(1), false),
        Arguments.of("0", BigInteger.valueOf(0), false),
        Arguments.of("-1", BigInteger.valueOf(-1), false),
        Arguments.of(
            Integer.toString(Integer.MAX_VALUE), BigInteger.valueOf(Integer.MAX_VALUE), false),
        Arguments.of(
            Integer.toString(Integer.MIN_VALUE), BigInteger.valueOf(Integer.MIN_VALUE), false),

        // decimals
        Arguments.of("3.14", BigDecimal.valueOf(3.14d), false),
        Arguments.of("-5.67", BigDecimal.valueOf(-5.67d), false),
        Arguments.of(
            Double.toString(Double.MAX_VALUE), BigDecimal.valueOf(Double.MAX_VALUE), false),
        Arguments.of(
            Double.toString(Double.MIN_VALUE), BigDecimal.valueOf(Double.MIN_VALUE), false),
        Arguments.of(
            Double.toString(Double.MIN_NORMAL), BigDecimal.valueOf(Double.MIN_NORMAL), false),

        // strings
        Arguments.of(" ", " ", true),
        Arguments.of("hello world", "hello world", true),
        Arguments.of("😍😎😺", "😍😎😺", true),
        Arguments.of("12345A", "12345A", true),
        Arguments.of("\uD83D\uDCA9ȇ", "\uD83D\uDCA9ȇ", true),

        // strings that look like other data types
        Arguments.of("2021-10-03", "2021-10-03", true),
        Arguments.of("2021-10-03T19:01:23", "2021-10-03T19:01:23", true),
        Arguments.of("terra-wds:/type/id", "terra-wds:/type/id", true),

        // TODO: these quoted-string tests are failing; fix this somehow.
        // the TSV parser automatically removes surrounding quotes, and our calling Java code
        // has no way to detect this happened. Thus, a quoted string of <"123"> gets processed as
        // <123>
        // and thus becomes a number. Instead, we want all quoted strings to become strings.
        // Arguments.of("\"\"",                    "",                     false),
        // Arguments.of("\"12345\"",               "12345",                false),
        // Arguments.of("\"true\"",                "true",                 false),
        // Arguments.of("\"false\"",               "false",                false),
        // Arguments.of("\"[1,2,3]\"",             "[1,2,3]",              false),

        // ========== arrays ==========

        // empty array
        Arguments.of("[]", Collections.EMPTY_LIST, false),

        // arrays of booleans
        Arguments.of("[true,false,true]", List.of(true, false, true), false),
        Arguments.of("[false, true, false]", List.of(false, true, false), false),

        // arrays of integers
        Arguments.of(
            "[" + Integer.MIN_VALUE + ", -1, 0, 1, " + Integer.MAX_VALUE + "]",
            List.of(
                BigInteger.valueOf(Integer.MIN_VALUE),
                BigInteger.valueOf(-1),
                BigInteger.valueOf(0),
                BigInteger.valueOf(1),
                BigInteger.valueOf(Integer.MAX_VALUE)),
            false),

        // arrays of decimals
        Arguments.of(
            "["
                + Double.MIN_VALUE
                + ", "
                + Double.MIN_NORMAL
                + ", -5.67, 3.14, "
                + Double.MAX_VALUE
                + "]",
            List.of(
                BigDecimal.valueOf(Double.MIN_VALUE),
                BigDecimal.valueOf(Double.MIN_NORMAL),
                BigDecimal.valueOf(-5.67),
                BigDecimal.valueOf(3.14),
                BigDecimal.valueOf(Double.MAX_VALUE)),
            false),

        // arrays of mixed numbers
        Arguments.of(
            "[4, 5.67, 8]",
            List.of(BigInteger.valueOf(4), BigDecimal.valueOf(5.67), BigInteger.valueOf(8)),
            false),

        // arrays of strings
        Arguments.of("[\" \", \"  \"]", List.of(" ", "  "), false),
        Arguments.of("[\"a\"]", List.of("a"), false),
        Arguments.of("[\"hello\", \"world\"]", List.of("hello", "world"), false),
        Arguments.of("[\"Hello\", \"World\"]", List.of("Hello", "World"), false),
        Arguments.of("[\"true\", \"false\"]", List.of("true", "false"), false),
        Arguments.of("[\"98\", \"99\"]", List.of("98", "99"), false),
        Arguments.of("[\"😍\", \"😎\", \"😺\"]", List.of("😍", "😎", "😺"), false),

        // arrays of strings that look like other data types
        Arguments.of(
            "[\"2021-10-03\", \"2022-11-04\"]", List.of("2021-10-03", "2022-11-04"), false),
        Arguments.of(
            "[\"2021-10-03T19:01:23\", \"2021-11-04T20:02:24\"]",
            List.of("2021-10-03T19:01:23", "2021-11-04T20:02:24"),
            false),
        Arguments.of(
            "[\"terra-wds:/type/id\", \"terra-wds:/type/id2\"]",
            List.of("terra-wds:/type/id", "terra-wds:/type/id2"),
            false),

        // mixed array (these deserialize as mixed lists, will be coerced to a single data type
        // later in processing)
        // TODO AJ-1839: json and TSV no longer deserialize mixed lists equally
        Arguments.of(
            "[\"hello\", 123, true]", List.of("hello", BigInteger.valueOf(123), true), false),
        Arguments.of("end of test cases", "end of test cases", true));
  }
}
