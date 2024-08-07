package org.databiosphere.workspacedataservice.search;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.List;
import java.util.Map;
import java.util.stream.Stream;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

class QueryParserTest {

  private static Stream<Arguments> singleColumnTerms() {
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

  // test expected parsing for a single column and its filter term
  @ParameterizedTest(name = "Valid query `column1:{0}`")
  @MethodSource("singleColumnTerms")
  void parseSingleColumnTerm(String queryTerm, String expectedResult) {
    String query = "column1:" + queryTerm;

    WhereClausePart actual = new QueryParser().parse(query);

    WhereClausePart expected =
        new WhereClausePart(
            List.of("\"column1\" = :filterquery0"),
            Map.of("filterquery0", expectedResult),
            List.of("column1"));

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
    QueryParser queryParser = new QueryParser();
    assertThrows(InvalidQueryException.class, () -> queryParser.parse(query));
  }
}
