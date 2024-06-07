package org.databiosphere.workspacedataservice.dao;

import bio.terra.common.db.ReadTransaction;
import java.util.Map;
import java.util.UUID;
import org.databiosphere.workspacedataservice.shared.model.RecordType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.dao.EmptyResultDataAccessException;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.lang.Nullable;
import org.springframework.stereotype.Repository;

/**
 * DAO to look up the name of the primary key for a given WDS RecordType. Users specify the PK for
 * each of their tables so WDS often needs to dynamically look it up.
 *
 * <p>A benefit of keeping this DAO separate from RecordDao is that at some point in the future, we
 * could re-enable caching for the getPrimaryKeyColumn method to eliminate lots of Postgres queries.
 *
 * <p>In order for @Cacheable to function properly callers need to be outside the class where
 * the @Cacheable method is specified. <a
 * href="https://stackoverflow.com/questions/16899604/spring-cache-cacheable-not-working-while-calling-from-another-method-of-the-s">https://stackoverflow.com/questions/16899604/spring-cache-cacheable-not-working-while-calling-from-another-method-of-the-s</a>
 * Many getPrimaryKeyColumn calls are in RecordDao thus that method can't belong to that class and
 * still work properly with caching. Any future db methods that should be cached that are invoked
 * from RecordDao can be added here as well.
 */
@Repository
public class PrimaryKeyDao {

  private final NamedParameterJdbcTemplate namedTemplate;

  private static final Logger LOGGER = LoggerFactory.getLogger(PrimaryKeyDao.class);

  public PrimaryKeyDao(NamedParameterJdbcTemplate namedTemplate) {
    this.namedTemplate = namedTemplate;
  }

  @Nullable
  private String getViewPrimaryKeyColumn(RecordType recordType, UUID instanceId) {
    // TODO: is this a view? Ideally this would reuse RecordDao.viewExists; if not reusable
    //    it should be combined into the query below
    if (!Boolean.TRUE.equals(
        namedTemplate.queryForObject(
            "select exists(select from pg_views where schemaname = :collectionId AND viewname  = :recordType)",
            new MapSqlParameterSource(
                Map.of("collectionId", instanceId.toString(), "recordType", recordType.getName())),
            Boolean.class))) {
      return null;
    }
    // TODO: views do not have primary keys. For now, just return the first column
    //    of the view as its primary key
    return namedTemplate.queryForObject(
        "select column_name from INFORMATION_SCHEMA.COLUMNS "
            + "where table_schema = :collectionId and table_name = :tableName limit 1",
        new MapSqlParameterSource(
            Map.of("collectionId", instanceId.toString(), "tableName", recordType.getName())),
        String.class);
  }

  /*
     AJ-1242: turning off caching for this method. This cache can return erroneous results in a
     multi-replica/horizontally-scaled environment. If one WDS replica deletes and recreates a table and uses
     a different primary key, another WDS replica would continue to respond with an out-of-date cache.

     If/when we re-enable caching for primary key values, this method will need a @Cacheable annotation. The
     cache should be keyed to the instanceId plus the recordType name.
  */
  @ReadTransaction
  public String getPrimaryKeyColumn(RecordType recordType, UUID instanceId) {
    try {
      String viewColumn = getViewPrimaryKeyColumn(recordType, instanceId);
      if (viewColumn != null) {
        return viewColumn;
      }
    } catch (EmptyResultDataAccessException emptyEx) {
      // noop; if this threw EmptyResultDataAccessException, it means the table is not a view
    }

    // AJ-1242: If/when we re-enable caching for primary key values, we may want a log statement
    // here to
    // show when we are reading directly from the db vs. reading from cache.

    MapSqlParameterSource params =
        new MapSqlParameterSource(
            "qTableName", SqlUtils.getQualifiedTableName(recordType, instanceId));

    // see https://wiki.postgresql.org/wiki/Retrieve_primary_key_columns for this query
    // for commentary, see also:
    // https://stackoverflow.com/questions/1214576/how-do-i-get-the-primary-keys-of-a-table-from-postgres-via-plpgsql
    return namedTemplate.queryForObject(
        """
                      SELECT a.attname
                      FROM   pg_index i
                      JOIN   pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = ANY(i.indkey)
                      WHERE  i.indrelid = :qTableName::regclass
                      AND    i.indisprimary;""",
        params,
        String.class);
  }
}
