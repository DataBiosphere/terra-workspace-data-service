package org.databiosphere.workspacedataservice.search;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;
import org.databiosphere.workspacedataservice.service.model.DataTypeMapping;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.MethodSource;

class QueryParserTest {

  // ========== STRING and array thereof

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
    scalarStringTestImpl(queryTerm, expectedResult, DataTypeMapping.STRING);
  }

  // test expected parsing for a single array-of-string column and its filter term
  @ParameterizedTest(name = "Valid query `column1:{0}`")
  @MethodSource("stringTerms")
  void parseSingleArrayOfStringColumnTerm(String queryTerm, String expectedResult) {
    arrayStringTestImpl(queryTerm, expectedResult, DataTypeMapping.ARRAY_OF_STRING);
  }

  // helper for testing scalar strings; reused for scalar files
  private void scalarStringTestImpl(
      String queryTerm, String expectedResult, DataTypeMapping dataType) {
    String query = "column1:" + queryTerm;

    WhereClausePart actual = new QueryParser(Map.of("column1", dataType)).parse(query);

    WhereClausePart expected =
        new WhereClausePart(
            List.of("LOWER(\"column1\") = :filterquery0"),
            Map.of("filterquery0", expectedResult.toLowerCase()));

    assertEquals(expected, actual);
  }

  // helper for testing array of strings; reused for array of files
  private void arrayStringTestImpl(
      String queryTerm, String expectedResult, DataTypeMapping dataType) {
    String query = "column1:" + queryTerm;

    WhereClausePart actual = new QueryParser(Map.of("column1", dataType)).parse(query);

    WhereClausePart expected =
        new WhereClausePart(
            List.of(":filterquery0 ILIKE ANY(\"column1\")"),
            Map.of("filterquery0", expectedResult.toLowerCase()));

    assertEquals(expected, actual);
  }

  // ========== FILE and array thereof
  private static Stream<Arguments> fileTerms() {
    return Stream.of(
        Arguments.of(
            "\"https://lz1a2b345c67def8a91234bc.blob.core.windows.net/sc-7ad51c5d-eb4c-4685-bffe-62b861f7753f/file.bam\"",
            "https://lz1a2b345c67def8a91234bc.blob.core.windows.net/sc-7ad51c5d-eb4c-4685-bffe-62b861f7753f/file.bam"),
        Arguments.of(
            "\"drs://example.org/dg.4503/cc32d93d-a73c-4d2c-a061-26c0410e74fa\"",
            "drs://example.org/dg.4503/cc32d93d-a73c-4d2c-a061-26c0410e74fa"),
        Arguments.of(
            "\"https://teststorageaccount.blob.core.windows.net/testcontainer/file\"",
            "https://teststorageaccount.blob.core.windows.net/testcontainer/file"));
  }

  // test expected parsing for a single string column and its filter term
  @ParameterizedTest(name = "Valid query `column1:{0}`")
  @MethodSource("fileTerms")
  void parseSingleFileColumnTerm(String queryTerm, String expectedResult) {
    scalarStringTestImpl(queryTerm, expectedResult, DataTypeMapping.FILE);
  }

  // test expected parsing for a single array-of-string column and its filter term
  @ParameterizedTest(name = "Valid query `column1:{0}`")
  @MethodSource("fileTerms")
  void parseSingleArrayOfFileColumnTerm(String queryTerm, String expectedResult) {
    arrayStringTestImpl(queryTerm, expectedResult, DataTypeMapping.ARRAY_OF_FILE);
  }

  // ========== RELATION and array thereof
  private static Stream<Arguments> relationTerms() {
    return Stream.of(
        Arguments.of("one", "one"),
        Arguments.of("\"terra-wds://target/one\"", "terra-wds://target/one"),
        Arguments.of("two", "two"),
        Arguments.of("\"terra-wds://target/two\"", "terra-wds://target/two"));
  }

  // test expected parsing for a single string column and its filter term
  @ParameterizedTest(name = "Valid query `column1:{0}`")
  @MethodSource("relationTerms")
  void parseSingleRelationColumnTerm(String queryTerm, String expectedResult) {
    scalarStringTestImpl(queryTerm, expectedResult, DataTypeMapping.RELATION);
  }

  // test expected parsing for a single string column and its filter term
  @ParameterizedTest(name = "Valid query `column1:{0}`")
  @MethodSource("relationTerms")
  void parseSingleArrayOfRelationColumnTerm(String queryTerm, String expectedResult) {
    String query = "column1:" + queryTerm;

    WhereClausePart actual =
        new QueryParser(Map.of("column1", DataTypeMapping.ARRAY_OF_RELATION)).parse(query);

    WhereClausePart expected =
        new WhereClausePart(
            List.of(
                ":filterquery0 IN (select LOWER(split_part(unnest, '/', 3)) from unnest(\"column1\"))"),
            Map.of("filterquery0", expectedResult.toLowerCase()));

    assertEquals(expected, actual);
  }

  // ========== JSON and array thereof
  private static Stream<Arguments> jsonTerms() {
    return Stream.of(
        Arguments.of("\"\\{\\\"foo\\\":1\\}\"", "{\"foo\":1}"),
        Arguments.of("\"\\{\\\"foo\\\":12, \\\"bar\\\":34\\}\"", "{\"foo\":12, \"bar\":34}"));
  }

  // test expected parsing for a single json column and its filter term
  @ParameterizedTest(name = "Valid query `column1:{0}`")
  @MethodSource("jsonTerms")
  void parseSingleJsonColumnTerm(String queryTerm, String expectedResult) {
    String query = "column1:" + queryTerm;

    WhereClausePart actual = new QueryParser(Map.of("column1", DataTypeMapping.JSON)).parse(query);

    WhereClausePart expected =
        new WhereClausePart(
            List.of("\"column1\" = :filterquery0::jsonb"),
            Map.of("filterquery0", expectedResult.toLowerCase()));

    assertEquals(expected, actual);
  }

  // test expected parsing for a single array-of-json column and its filter term
  @ParameterizedTest(name = "Valid query `column1:{0}`")
  @MethodSource("jsonTerms")
  void parseSingleArrayOfJsonColumnTerm(String queryTerm, String expectedResult) {
    String query = "column1:" + queryTerm;

    WhereClausePart actual =
        new QueryParser(Map.of("column1", DataTypeMapping.ARRAY_OF_JSON)).parse(query);

    WhereClausePart expected =
        new WhereClausePart(
            List.of(":filterquery0::jsonb = ANY(\"column1\")"),
            Map.of("filterquery0", expectedResult));

    assertEquals(expected, actual);
  }

  // ========== NUMBER and array thereof

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

  // ========== BOOLEAN and array thereof

  private static Stream<Arguments> booleanTerms() {
    return Stream.of(
        Arguments.of("true", true),
        Arguments.of("True", true),
        Arguments.of("TRUE", true),
        Arguments.of("false", false),
        Arguments.of("False", false),
        Arguments.of("FALSE", false));
  }

  // test expected parsing for a single boolean column and its filter term
  @ParameterizedTest(name = "Valid query `column1:{0}`")
  @MethodSource("booleanTerms")
  void parseSingleBooleanColumnTerm(String queryTerm, boolean expectedResult) {
    String query = "column1:" + queryTerm;

    WhereClausePart actual =
        new QueryParser(Map.of("column1", DataTypeMapping.BOOLEAN)).parse(query);

    WhereClausePart expected =
        new WhereClausePart(
            List.of("\"column1\" = :filterquery0"), Map.of("filterquery0", expectedResult));

    assertEquals(expected, actual);
  }

  // test expected parsing for a single array-of-boolean column and its filter term
  @ParameterizedTest(name = "Valid query `column1:{0}`")
  @MethodSource("booleanTerms")
  void parseSingleArrayOfBooleanColumnTerm(String queryTerm, boolean expectedResult) {
    String query = "column1:" + queryTerm;

    WhereClausePart actual =
        new QueryParser(Map.of("column1", DataTypeMapping.ARRAY_OF_BOOLEAN)).parse(query);

    WhereClausePart expected =
        new WhereClausePart(
            List.of(":filterquery0 = ANY(\"column1\")"), Map.of("filterquery0", expectedResult));

    assertEquals(expected, actual);
  }

  // ========== DATE and array thereof

  private static Stream<Arguments> dateTerms() {
    return Stream.of(
        Arguments.of("1979-06-25", LocalDate.of(1979, 6, 25)),
        Arguments.of("1981-02-12", LocalDate.of(1981, 2, 12)));
  }

  // test expected parsing for a single date column and its filter term
  @ParameterizedTest(name = "Valid query `column1:{0}`")
  @MethodSource("dateTerms")
  void parseSingleDateColumnTerm(String queryTerm, LocalDate expectedResult) {
    String query = "column1:" + queryTerm;

    WhereClausePart actual = new QueryParser(Map.of("column1", DataTypeMapping.DATE)).parse(query);

    WhereClausePart expected =
        new WhereClausePart(
            List.of("\"column1\" = :filterquery0"), Map.of("filterquery0", expectedResult));

    assertEquals(expected, actual);
  }

  // test expected parsing for a single array-of-date column and its filter term
  @ParameterizedTest(name = "Valid query `column1:{0}`")
  @MethodSource("dateTerms")
  void parseSingleArrayOfDateColumnTerm(String queryTerm, LocalDate expectedResult) {
    String query = "column1:" + queryTerm;

    WhereClausePart actual =
        new QueryParser(Map.of("column1", DataTypeMapping.ARRAY_OF_DATE)).parse(query);

    WhereClausePart expected =
        new WhereClausePart(
            List.of(":filterquery0 = ANY(\"column1\")"), Map.of("filterquery0", expectedResult));

    assertEquals(expected, actual);
  }

  // ========== DATETIME and array thereof

  private static Stream<Arguments> datetimeTerms() {
    return Stream.of(
        Arguments.of("\"2024-08-13T19:00:00\"", LocalDateTime.of(2024, 8, 13, 19, 0)),
        Arguments.of("\"2024-08-08T14:00:00\"", LocalDateTime.of(2024, 8, 8, 14, 0)));
  }

  // test expected parsing for a single datetime column and its filter term
  @ParameterizedTest(name = "Valid query `column1:{0}`")
  @MethodSource("datetimeTerms")
  void parseSingleDatetimeColumnTerm(String queryTerm, LocalDateTime expectedResult) {
    String query = "column1:" + queryTerm;

    WhereClausePart actual =
        new QueryParser(Map.of("column1", DataTypeMapping.DATE_TIME)).parse(query);

    WhereClausePart expected =
        new WhereClausePart(
            List.of("\"column1\" = :filterquery0"), Map.of("filterquery0", expectedResult));

    assertEquals(expected, actual);
  }

  // test expected parsing for a single array-of-datetime column and its filter term
  @ParameterizedTest(name = "Valid query `column1:{0}`")
  @MethodSource("datetimeTerms")
  void parseSingleArrayOfDatetimeColumnTerm(String queryTerm, LocalDateTime expectedResult) {
    String query = "column1:" + queryTerm;

    WhereClausePart actual =
        new QueryParser(Map.of("column1", DataTypeMapping.ARRAY_OF_DATE_TIME)).parse(query);

    WhereClausePart expected =
        new WhereClausePart(
            List.of(":filterquery0 = ANY(\"column1\")"), Map.of("filterquery0", expectedResult));

    assertEquals(expected, actual);
  }

  // ========== NULL, EMPTY_ARRAY
  @ParameterizedTest(name = "Query on `{0}` column")
  @EnumSource(
      value = DataTypeMapping.class,
      names = {"NULL", "EMPTY_ARRAY"})
  void nullAndEmptyArrayColumns(DataTypeMapping dataType) {
    String query = "column1:whatever";

    WhereClausePart actual = new QueryParser(Map.of("column1", dataType)).parse(query);

    WhereClausePart expected = new WhereClausePart(List.of("false"), Map.of());

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
