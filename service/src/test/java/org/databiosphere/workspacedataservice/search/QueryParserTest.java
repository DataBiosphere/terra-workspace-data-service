package org.databiosphere.workspacedataservice.search;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.List;
import java.util.Map;
import java.util.stream.Stream;
import org.databiosphere.workspacedataservice.service.model.DataTypeMapping;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

class QueryParserTest {

  private static Stream<Arguments> stringTerms() {
    return Stream.of(
        Arguments.of("foo", "foo"),
        // Lucene query syntax uses * and ? as wildcards
        Arguments.of("foo*", "foo*"),
        Arguments.of("fo?o", "fo?o"),
        // Wildcards can be escaped
        Arguments.of("fo\\?o\\*", "fo?o*"),
        // SQL "like" uses % as a wildcard. The SQL we currently generate does not use "like", so
        // end users can input a wildcard, but it won't have the effect they may have wanted
        Arguments.of("%foo%", "%foo%"),
        // whitespace in terms requires quoting the term
        Arguments.of("\"this is a phrase\"", "this is a phrase"),
        // because the query is a string, numbers come through as strings too
        Arguments.of("42", "42"));
  }

  // test expected parsing for a single string column and its filter term
  @ParameterizedTest(name = "Valid query `column1:{0}`")
  @MethodSource("stringTerms")
  void parseSingleStringColumnTerm(String queryTerm, String expectedResult) {
    String query = "column1:" + queryTerm;

    WhereClausePart actual =
        new QueryParser(Map.of("column1", DataTypeMapping.STRING)).parse(query);

    WhereClausePart expected =
        new WhereClausePart(
            List.of("LOWER(\"column1\") = :filterquery0"),
            Map.of("filterquery0", expectedResult.toLowerCase()));

    assertEquals(expected, actual);
  }

  // test expected parsing for a single array-of-string column and its filter term
  @ParameterizedTest(name = "Valid query `column1:{0}`")
  @MethodSource("stringTerms")
  void parseSingleArrayOfStringColumnTerm(String queryTerm, String expectedResult) {
    String query = "column1:" + queryTerm;

    WhereClausePart actual =
        new QueryParser(Map.of("column1", DataTypeMapping.ARRAY_OF_STRING)).parse(query);

    WhereClausePart expected =
        new WhereClausePart(
            List.of(":filterquery0 ILIKE ANY(\"column1\")"),
            Map.of("filterquery0", expectedResult.toLowerCase()));

    assertEquals(expected, actual);
  }

  private static Stream<Arguments> numberTerms() {
    return Stream.of(
        Arguments.of("1", 1d),
        Arguments.of("1.23", 1.23d),
        Arguments.of(Double.toString(Double.MAX_VALUE), Double.MAX_VALUE),
        Arguments.of(Double.toString(Double.MIN_VALUE), Double.MIN_VALUE),
        // negative numbers require escaping the dash; a bare dash is shorthand for NOT:
        // https://lucene.apache.org/core/2_9_4/queryparsersyntax.html#Escaping%20Special%20Characters
        Arguments.of("\\-1.23", -1.23d));
  }

  // test expected parsing for a single number column and its filter term
  @ParameterizedTest(name = "Valid query `column1:{0}`")
  @MethodSource("numberTerms")
  void parseSingleNumberColumnTerm(String queryTerm, Double expectedResult) {
    String query = "column1:" + queryTerm;

    WhereClausePart actual =
        new QueryParser(Map.of("column1", DataTypeMapping.NUMBER)).parse(query);

    WhereClausePart expected =
        new WhereClausePart(
            List.of("\"column1\" = :filterquery0"), Map.of("filterquery0", expectedResult));

    assertEquals(expected, actual);
  }

  // test expected parsing for a single array-of-number column and its filter term
  @ParameterizedTest(name = "Valid query `column1:{0}`")
  @MethodSource("numberTerms")
  void parseSingleArrayOfNumberColumnTerm(String queryTerm, Double expectedResult) {
    String query = "column1:" + queryTerm;

    WhereClausePart actual =
        new QueryParser(Map.of("column1", DataTypeMapping.ARRAY_OF_NUMBER)).parse(query);

    WhereClausePart expected =
        new WhereClausePart(
            List.of(":filterquery0 = ANY(\"column1\")"), Map.of("filterquery0", expectedResult));

    assertEquals(expected, actual);
  }

  private static Stream<String> invalidQuerySyntax() {
    return Stream.of(
        // ranges
        "column1:[23 TO 45]",
        // multi-column search
        "column1:foo AND column2:bar",
        // table-wide search, i.e. no column specified
        "searchterm");
  }

  // we only support a subset of Lucene query parser syntax. These test cases are valid for
  // Lucene, but will throw a InvalidQueryException because WDS doesn't support them
  @ParameterizedTest(name = "Invalid query `{0}`")
  @MethodSource("invalidQuerySyntax")
  void parseInvalidQueries(String query) {
    QueryParser queryParser = new QueryParser(Map.of("column1", DataTypeMapping.STRING));
    assertThrows(InvalidQueryException.class, () -> queryParser.parse(query));
  }
}
