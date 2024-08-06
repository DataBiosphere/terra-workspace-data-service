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
  void parseSingleWildcard() {
    String query = "column1:fo?o*";
    assertThrows(InvalidQueryException.class, () -> new QueryParser().parse(query));
  }

  @Test
  void parseSqlWildcard() {
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
    String query = "column1:42 AND column2:314159";
    assertThrows(InvalidQueryException.class, () -> new QueryParser().parse(query));
  }

  @Test
  void parseNoFieldSpecified() {
    String query = "searchterm";
    assertThrows(InvalidQueryException.class, () -> new QueryParser().parse(query));
  }
}
