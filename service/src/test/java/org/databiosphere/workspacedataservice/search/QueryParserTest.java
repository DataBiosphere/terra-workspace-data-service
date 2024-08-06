package org.databiosphere.workspacedataservice.search;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

class QueryParserTest {

  @Test
  void parseSingleField() {
    String query = "column1:foo";

    WhereClausePart actual = new QueryParser().parse(query);

    WhereClausePart expected =
        new WhereClausePart(
            List.of("\"column1\" = :filterquery0"),
            Map.of("filterquery0", "foo"),
            List.of("column1"));

    assertEquals(expected, actual);
  }

  @Test
  void parseStarWildcard() {
    String query = "column1:foo*";
    WhereClausePart actual = new QueryParser().parse(query);

    WhereClausePart expected =
        new WhereClausePart(
            List.of("\"column1\" = :filterquery0"),
            Map.of("filterquery0", "foo*"),
            List.of("column1"));

    assertEquals(expected, actual);
  }

  @Test
  void parseQuestionWildcard() {
    String query = "column1:fo?o";
    WhereClausePart actual = new QueryParser().parse(query);

    WhereClausePart expected =
        new WhereClausePart(
            List.of("\"column1\" = :filterquery0"),
            Map.of("filterquery0", "fo?o"),
            List.of("column1"));

    assertEquals(expected, actual);
  }

  @Test
  void parseEscapedWildcards() {
    String query = "column1:fo\\?o\\*";
    WhereClausePart actual = new QueryParser().parse(query);

    WhereClausePart expected =
        new WhereClausePart(
            List.of("\"column1\" = :filterquery0"),
            Map.of("filterquery0", "fo?o*"),
            List.of("column1"));

    assertEquals(expected, actual);
  }

  @Test
  void parseSqlWildcard() {
    // note this is allowed. The SQL where clause we generate will not respect the wildcard;
    // it does not use LIKE. So even though we allow this, it won't have the effect the user
    // desired.
    String query = "column1:%foo%";
    WhereClausePart actual = new QueryParser().parse(query);

    WhereClausePart expected =
        new WhereClausePart(
            List.of("\"column1\" = :filterquery0"),
            Map.of("filterquery0", "%foo%"),
            List.of("column1"));

    assertEquals(expected, actual);
  }

  @Test
  void parseSingleQuoted() {
    String query = "column1:\"this is a phrase\"";

    WhereClausePart actual = new QueryParser().parse(query);

    WhereClausePart expected =
        new WhereClausePart(
            List.of("\"column1\" = :filterquery0"),
            Map.of("filterquery0", "this is a phrase"),
            List.of("column1"));

    assertEquals(expected, actual);
  }

  @Test
  void parseSingleNumber() {
    String query = "column1:42";

    WhereClausePart actual = new QueryParser().parse(query);

    WhereClausePart expected =
        new WhereClausePart(
            List.of("\"column1\" = :filterquery0"),
            Map.of("filterquery0", "42"),
            List.of("column1"));

    assertEquals(expected, actual);
  }

  @Test
  void parseSingleNumberRange() {
    String query = "column1:[23 TO 45]";
    assertThrows(InvalidQueryException.class, () -> new QueryParser().parse(query));
  }

  @Test
  void parseMultipleFields() {
    String query = "column1:foo AND column2:bar";
    assertThrows(InvalidQueryException.class, () -> new QueryParser().parse(query));
  }

  @Test
  void parseNoFieldSpecified() {
    String query = "searchterm";
    assertThrows(InvalidQueryException.class, () -> new QueryParser().parse(query));
  }
}
