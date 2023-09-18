package org.databiosphere.workspacedataservice.dao;

import org.databiosphere.workspacedataservice.shared.model.RecordType;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.stereotype.Repository;

import java.util.UUID;

/**
 * In order for @Cacheable to function properly callers need to be outside the class where
 * the @Cacheable method is specified.
 * <a href="https://stackoverflow.com/questions/16899604/spring-cache-cacheable-not-working-while-calling-from-another-method-of-the-s">https://stackoverflow.com/questions/16899604/spring-cache-cacheable-not-working-while-calling-from-another-method-of-the-s</a>
 * Many getPrimaryKeyColumn calls are in RecordDao thus that method can't belong to that class and still work properly with caching,
 * so it's been moved here.  Any future db methods that should be cached that are invoked from RecordDao can be added here as well.
 */
@Repository
public class CachedQueryDao {

    private final NamedParameterJdbcTemplate namedTemplate;

    public CachedQueryDao(NamedParameterJdbcTemplate namedTemplate) {
        this.namedTemplate = namedTemplate;
    }

    /*
        David An 2023-09-15: turning off caching for this method. This cache can return erroneous results in a
        multi-replica/horizontally-scaled environment. If one WDS replica deletes and recreates a table and uses
        a different primary key, another WDS replica would continue to respond with an out-of-date cache.

        We expect to turn this cache back on at some point, so I am leaving the "CachedQueryDao" class name
        and the @Cacheable annotation in place but commented out.
     */
    // @Cacheable(value = PRIMARY_KEY_COLUMN_CACHE, key = "{ #recordType.name, #instanceId.toString()}")
    public String getPrimaryKeyColumn(RecordType recordType, UUID instanceId) {
        // David An 2023-09-15: this log line is only relevant if the cache is enabled. Also comment it out for now.
        // LOGGER.warn("Calling the db to retrieve primary key for {}.{}", instanceId, recordType.getName());
        MapSqlParameterSource params = new MapSqlParameterSource("qTableName", SqlUtils.getQualifiedTableName(recordType, instanceId));

        // see https://wiki.postgresql.org/wiki/Retrieve_primary_key_columns for this query
        // for commentary, see also:
        // https://stackoverflow.com/questions/1214576/how-do-i-get-the-primary-keys-of-a-table-from-postgres-via-plpgsql
        return namedTemplate.queryForObject("""
                      SELECT a.attname
                      FROM   pg_index i
                      JOIN   pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = ANY(i.indkey)
                      WHERE  i.indrelid = :qTableName::regclass
                      AND    i.indisprimary;""",
                params, String.class);
    }
}
