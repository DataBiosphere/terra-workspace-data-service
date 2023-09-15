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
 * Many getPrimaryKeyColumn calls are in RecordDao thus that method can't belong to that class and still work properly with caching
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
    public String getPrimaryKeyColumn(RecordType recordType, UUID instanceId){
        // David An 2023-09-15: this log line is only relevant if the cache is enabled. Also comment it out for now.
        // LOGGER.warn("Calling the db to retrieve primary key for {}.{}", instanceId, recordType.getName());
        MapSqlParameterSource params = new MapSqlParameterSource("recordType", recordType.getName());
        params.addValue("instanceId", instanceId.toString());
        return namedTemplate.queryForObject("select kcu.column_name FROM information_schema.table_constraints tc " +
                        "JOIN information_schema.key_column_usage kcu ON tc.constraint_name = kcu.constraint_name AND tc.table_schema = kcu.table_schema " +
                        "JOIN information_schema.constraint_column_usage ccu ON ccu.constraint_name = tc.constraint_name AND ccu.table_schema = tc.table_schema " +
                        "WHERE tc.constraint_type = 'PRIMARY KEY' AND tc.table_schema = :instanceId AND tc.table_name = :recordType",
                params, String.class);
    }
}
