package org.databiosphere.workspacedataservice.tsv;

import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.ArgumentsProvider;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.AbstractMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

public class TsvArgumentsProvider implements ArgumentsProvider {

    // TODO: flesh out these test cases!!

    Map<String, Boolean> booleans = Map.of(
            "true", true,
            "false", false
    );

    Map<String, BigDecimal> bigDecimals = Map.ofEntries(
            new AbstractMap.SimpleEntry<>("3.14", BigDecimal.valueOf(3.14d)),
            new AbstractMap.SimpleEntry<>("-5.67", BigDecimal.valueOf(-5.67d)),
            new AbstractMap.SimpleEntry<>(Double.toString(Double.MAX_VALUE), BigDecimal.valueOf(Double.MAX_VALUE)),
            new AbstractMap.SimpleEntry<>(Double.toString(Double.MIN_VALUE), BigDecimal.valueOf(Double.MIN_VALUE)),
            new AbstractMap.SimpleEntry<>(Double.toString(Double.MIN_NORMAL), BigDecimal.valueOf(Double.MIN_NORMAL))
    );

    Map<String, BigInteger> bigIntegers = Map.ofEntries(
            new AbstractMap.SimpleEntry<>("1", BigInteger.valueOf(1)),
            new AbstractMap.SimpleEntry<>("0", BigInteger.valueOf(0)),
            new AbstractMap.SimpleEntry<>("-1", BigInteger.valueOf(-1)),
            new AbstractMap.SimpleEntry<>(Integer.toString(Integer.MAX_VALUE), BigInteger.valueOf(Integer.valueOf(Integer.MAX_VALUE).longValue())),
            new AbstractMap.SimpleEntry<>(Integer.toString(Integer.MIN_VALUE), BigInteger.valueOf(Integer.valueOf(Integer.MIN_VALUE).longValue()))
    );

    // note that we do not parse dates, datetimes, or relations in the first stage of TSV or JSON
    // deserialization - that detection happens later. So we include those inputs here as simple strings.
    List<String> strings = List.of(
      "hello world",
      "foo",
      "bar",
      "2021-10-03",
      "2021-10-03T19:01:23",
      "terra-wds:/target-record/record_0"
    );

    Map<String,List<BigInteger>> arraysOfInts = Map.of(
            "[1, 2, 3]", List.of(BigInteger.valueOf(1), BigInteger.valueOf(2), BigInteger.valueOf(3))
    );

    Map<String,List<String>> arraysOfStrings = Map.of(
            "[\"hello\", \"world\"]", List.of("hello", "world"),
            "[\"98\", \"99\"]", List.of("98", "99")
    );


    @Override
    public Stream<? extends Arguments> provideArguments(ExtensionContext context) throws Exception {
        /* Arguments are sets:
			- first value is the text that would be contained in a TSV cell or JSON value
			- second value is the expected Java type that the TsvConverter or JSON deserializer would create for that cell
			- third value, a boolean, indicates whether the value should be quoted inside a JSON packet
		*/


        Stream<Arguments> booleanArguments = booleans.entrySet().stream().map( entry ->
                Arguments.of(entry.getKey(), entry.getValue(), false));

        Stream<Arguments> bigIntegerArguments = bigIntegers.entrySet().stream().map( entry ->
                Arguments.of(entry.getKey(), entry.getValue(), false));

        Stream<Arguments> bigDecimalArguments = bigDecimals.entrySet().stream().map( entry ->
                Arguments.of(entry.getKey(), entry.getValue(), false));

        Stream<Arguments> stringArguments = strings.stream().map( input ->
                Arguments.of(input, input, true));

        Stream<Arguments> arrayOfIntsArguments = arraysOfInts.entrySet().stream().map( entry ->
                Arguments.of(entry.getKey(), entry.getValue(), false));

        Stream<Arguments> arrayOfStringsArguments = arraysOfStrings.entrySet().stream().map( entry ->
                Arguments.of(entry.getKey(), entry.getValue(), false));



        return Stream.of(booleanArguments, bigIntegerArguments, bigDecimalArguments, stringArguments,
                        arrayOfIntsArguments, arrayOfStringsArguments)
                .reduce(Stream::concat)
                .orElseGet(Stream::empty);

//        return Stream.of(
//                Arguments.of("[\"test1\", \"test2\"]", Arrays.asList("test1", "test2"), false),
//                Arguments.of("[“test1”, “test2”]", Arrays.asList("test1", "test2"), false), // smart quotes - will fail for JSON
//                Arguments.of("[\"98\", \"99\"]", Arrays.asList("98", "99"), false),
//                Arguments.of("[true, false, true]", Arrays.asList(true, false, true), false),
//                Arguments.of(" ", " ", true),
//        );
    }
}
