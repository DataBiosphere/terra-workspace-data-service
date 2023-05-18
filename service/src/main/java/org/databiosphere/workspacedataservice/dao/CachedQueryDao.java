package org.databiosphere.workspacedataservice.dao;

import org.databiosphere.workspacedataservice.shared.model.RecordType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.cache.annotation.Cacheable;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.stereotype.Repository;

import java.util.UUID;

import static org.databiosphere.workspacedataservice.service.model.ReservedNames.PRIMARY_KEY_COLUMN_CACHE;

/**
 * In order for @Cacheable to function properly callers need to be outside the class where
 * the @Cacheable method is specified.
 * <a href="https://stackoverflow.com/questions/16899604/spring-cache-cacheable-not-working-while-calling-from-another-method-of-the-s">...</a>
 * Many getPrimaryKeyColumn calls are in RecordDao thus that method can't belong to that class and still work properly with caching
 * so it's been moved here.  Any future db methods that should be cached that are invoked from RecordDao can be added here as well.
 */
@Repository
public class CachedQueryDao {

    private static final Logger LOGGER = LoggerFactory.getLogger(CachedQueryDao.class);

    private final NamedParameterJdbcTemplate namedTemplate;

    public CachedQueryDao(NamedParameterJdbcTemplate namedTemplate) {
        this.namedTemplate = namedTemplate;
    }

    @Cacheable(value = PRIMARY_KEY_COLUMN_CACHE, key = "{ #recordType.name, #instanceId.toString()}")
    public String getPrimaryKeyColumn(RecordType recordType, UUID instanceId){
        LOGGER.warn("Calling the db to retrieve primary key for {}.{}", instanceId, recordType.getName());
        MapSqlParameterSource params = new MapSqlParameterSource("recordType", recordType.getName());
        params.addValue("instanceId", instanceId.toString());
        return namedTemplate.queryForObject("select kcu.column_name FROM information_schema.table_constraints tc " +
                        "JOIN information_schema.key_column_usage kcu ON tc.constraint_name = kcu.constraint_name AND tc.table_schema = kcu.table_schema " +
                        "JOIN information_schema.constraint_column_usage ccu ON ccu.constraint_name = tc.constraint_name AND ccu.table_schema = tc.table_schema " +
                        "WHERE tc.constraint_type = 'PRIMARY KEY' AND tc.table_schema = :instanceId AND tc.table_name = :recordType",
                params, String.class);
    }
}
