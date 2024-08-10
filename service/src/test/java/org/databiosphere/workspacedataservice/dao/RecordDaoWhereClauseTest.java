package org.databiosphere.workspacedataservice.dao;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.databiosphere.workspacedataservice.search.WhereClause;
import org.databiosphere.workspacedataservice.service.model.DataTypeMapping;
import org.databiosphere.workspacedataservice.shared.model.SearchFilter;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

/**
 * Tests for RecordDao.generateQueryWhereClause() These tests don't require any Spring context, so
 * it's nice to have them in their own class
 *
 * <p>See also org.databiosphere.workspacedataservice.search.QueryParserTest. QueryParserTest has
 * detailed tests for the filter.query portion of the where clause, and we don't need to replicate
 * those here. Here, we should be testing the interactions between filter.query and filter.ids, as
 * well as any other higher-level logic.
 */
class RecordDaoWhereClauseTest {

  @Test
  void emptyClause() {
    WhereClause actual =
        RecordDao.generateQueryWhereClause("my-pk-col", Map.of(), Optional.empty());
    assertEquals("", actual.sql());
    assertEquals(Map.of(), actual.params().getValues());
  }

  @Test
  void oneFilterId() {
    List<String> ids = List.of("one");
    SearchFilter searchFilter = new SearchFilter(Optional.of(ids), Optional.empty());

    WhereClause actual =
        RecordDao.generateQueryWhereClause("my-pk-col", Map.of(), Optional.of(searchFilter));
    assertEquals(" where \"my-pk-col\" in (:filterIds)", actual.sql());
    assertEquals(Map.of("filterIds", ids), actual.params().getValues());
  }

  @Test
  void multipleFilterIds() {
    List<String> ids = List.of("one", "two", "three");
    SearchFilter searchFilter = new SearchFilter(Optional.of(ids), Optional.empty());

    WhereClause actual =
        RecordDao.generateQueryWhereClause("my-pk-col", Map.of(), Optional.of(searchFilter));
    assertEquals(" where \"my-pk-col\" in (:filterIds)", actual.sql());
    assertEquals(Map.of("filterIds", ids), actual.params().getValues());
  }

  @Test
  void emptyFilterQuery() {
    SearchFilter searchFilter = new SearchFilter(Optional.empty(), Optional.of(""));

    WhereClause actual =
        RecordDao.generateQueryWhereClause("my-pk-col", Map.of(), Optional.of(searchFilter));
    assertEquals("", actual.sql());
    assertEquals(Map.of(), actual.params().getValues());
  }

  @Test
  void oneColumnFilterQuery() {
    SearchFilter searchFilter = new SearchFilter(Optional.empty(), Optional.of("col1:col1value"));

    WhereClause actual =
        RecordDao.generateQueryWhereClause(
            "my-pk-col", Map.of("col1", DataTypeMapping.STRING), Optional.of(searchFilter));
    assertEquals(" where LOWER(\"col1\") = :filterquery0", actual.sql());
    assertEquals(Map.of("filterquery0", "col1value"), actual.params().getValues());
  }

  @Test
  void idsAndOneColumnFilterQuery() {
    List<String> ids = List.of("one", "two", "three");
    SearchFilter searchFilter = new SearchFilter(Optional.of(ids), Optional.of("col1:col1value"));

    WhereClause actual =
        RecordDao.generateQueryWhereClause(
            "my-pk-col", Map.of("col1", DataTypeMapping.STRING), Optional.of(searchFilter));
    assertEquals(
        " where \"my-pk-col\" in (:filterIds) and LOWER(\"col1\") = :filterquery0", actual.sql());
    assertEquals(
        Map.of("filterIds", ids, "filterquery0", "col1value"), actual.params().getValues());
  }

  @Disabled("we don't support multiple columns yet")
  @Test
  void idsAndMultipleColumnFiltersQuery() {
    List<String> ids = List.of("one", "two", "three");
    SearchFilter searchFilter =
        new SearchFilter(
            Optional.of(ids), Optional.of("col1:col1value AND col2:col2value AND col3:col3value"));

    WhereClause actual =
        RecordDao.generateQueryWhereClause("my-pk-col", Map.of(), Optional.of(searchFilter));
    assertEquals(
        " where \"my-pk-col\" in (:filterIds) and \"col1\" = :filter0 and \"col2\" = :filter1 and \"col3\" = :filter2",
        actual.sql());
    assertEquals(
        Map.of(
            "filterIds",
            ids,
            "filter0",
            "col1value",
            "filter1",
            "col2value",
            "filter2",
            "col3value"),
        actual.params().getValues());
  }
}
