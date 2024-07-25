package org.databiosphere.workspacedataservice.dao;

import static org.databiosphere.workspacedataservice.dao.RecordDao.WhereClause;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.databiosphere.workspacedata.model.FilterColumn;
import org.databiosphere.workspacedataservice.shared.model.SearchFilter;
import org.junit.jupiter.api.Test;

/**
 * Tests for RecordDao.generateQueryWhereClause() These tests don't require any Spring context, so
 * it's nice to have them in their own class
 */
class RecordDaoWhereClauseTest {

  @Test
  void emptyClause() {
    WhereClause actual = RecordDao.generateQueryWhereClause("my-pk-col", Optional.empty());
    assertEquals("", actual.sql());
    assertEquals(Map.of(), actual.params().getValues());
  }

  @Test
  void oneFilterId() {
    List<String> ids = List.of("one");
    SearchFilter searchFilter = new SearchFilter(Optional.of(ids), Optional.empty());

    WhereClause actual = RecordDao.generateQueryWhereClause("my-pk-col", Optional.of(searchFilter));
    assertEquals(" where \"my-pk-col\" in (:filterIds)", actual.sql());
    assertEquals(Map.of("filterIds", ids), actual.params().getValues());
  }

  @Test
  void multipleFilterIds() {
    List<String> ids = List.of("one", "two", "three");
    SearchFilter searchFilter = new SearchFilter(Optional.of(ids), Optional.empty());

    WhereClause actual = RecordDao.generateQueryWhereClause("my-pk-col", Optional.of(searchFilter));
    assertEquals(" where \"my-pk-col\" in (:filterIds)", actual.sql());
    assertEquals(Map.of("filterIds", ids), actual.params().getValues());
  }

  @Test
  void emptyColumnFilters() {
    List<FilterColumn> filterColumns = List.of();
    SearchFilter searchFilter = new SearchFilter(Optional.empty(), Optional.of(filterColumns));

    WhereClause actual = RecordDao.generateQueryWhereClause("my-pk-col", Optional.of(searchFilter));
    assertEquals("", actual.sql());
    assertEquals(Map.of(), actual.params().getValues());
  }

  @Test
  void oneColumnFilter() {
    List<FilterColumn> filterColumns = List.of(new FilterColumn().column("col1").find("col1value"));
    SearchFilter searchFilter = new SearchFilter(Optional.empty(), Optional.of(filterColumns));

    WhereClause actual = RecordDao.generateQueryWhereClause("my-pk-col", Optional.of(searchFilter));
    assertEquals(" where \"col1\" = :filter0", actual.sql());
    assertEquals(Map.of("filter0", "col1value"), actual.params().getValues());
  }

  @Test
  void multipleColumnFilters() {
    List<FilterColumn> filterColumns =
        List.of(
            new FilterColumn().column("col1").find("col1value"),
            new FilterColumn().column("col2").find("col2value"),
            new FilterColumn().column("col3").find("col3value"));
    SearchFilter searchFilter = new SearchFilter(Optional.empty(), Optional.of(filterColumns));

    WhereClause actual = RecordDao.generateQueryWhereClause("my-pk-col", Optional.of(searchFilter));
    assertEquals(
        " where \"col1\" = :filter0 and \"col2\" = :filter1 and \"col3\" = :filter2", actual.sql());
    assertEquals(
        Map.of("filter0", "col1value", "filter1", "col2value", "filter2", "col3value"),
        actual.params().getValues());
  }

  @Test
  void idsAndColumnFilters() {
    List<String> ids = List.of("one", "two", "three");
    List<FilterColumn> filterColumns =
        List.of(
            new FilterColumn().column("col1").find("col1value"),
            new FilterColumn().column("col2").find("col2value"),
            new FilterColumn().column("col3").find("col3value"));
    SearchFilter searchFilter = new SearchFilter(Optional.of(ids), Optional.of(filterColumns));

    WhereClause actual = RecordDao.generateQueryWhereClause("my-pk-col", Optional.of(searchFilter));
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
