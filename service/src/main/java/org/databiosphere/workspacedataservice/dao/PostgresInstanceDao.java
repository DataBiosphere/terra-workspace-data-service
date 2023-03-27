package org.databiosphere.workspacedataservice.dao;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.stereotype.Repository;

import java.util.List;
import java.util.Objects;
import java.util.UUID;

import static org.databiosphere.workspacedataservice.dao.SqlUtils.quote;

@Repository
public class PostgresInstanceDao implements InstanceDao {

    private static final Logger LOGGER = LoggerFactory.getLogger(PostgresInstanceDao.class);

    @Value("${spring.datasource.username}")
    private String wdsDbUser;

    private final NamedParameterJdbcTemplate namedTemplate;

    public PostgresInstanceDao(NamedParameterJdbcTemplate namedTemplate) {
        this.namedTemplate = namedTemplate;
    }

    @Override
    public boolean instanceSchemaExists(UUID instanceId) {
        return Boolean.TRUE.equals(namedTemplate.queryForObject(
                "select exists(select from information_schema.schemata WHERE schema_name = :workspaceSchema)",
                new MapSqlParameterSource("workspaceSchema", instanceId.toString()), Boolean.class));
    }

    @Override
    public List<UUID> listInstanceSchemas() {
        List<String> schemas = namedTemplate.getJdbcTemplate()
                .queryForList("select schema_name from information_schema.schemata " +
                                "where schema_owner = ? order by schema_name",
                        String.class, wdsDbUser);
        // WDS only allows creation of schemas that are UUIDs
        return schemas.stream().map(s -> safeParseUUID(s))
                .filter(Objects::nonNull).toList();
    }

    @Override
    @SuppressWarnings("squid:S2077") // since instanceId must be a UUID, it is safe to use inline
    public void createSchema(UUID instanceId) {
        namedTemplate.getJdbcTemplate().update("create schema " + quote(instanceId.toString()));
    }


    @Override
    @SuppressWarnings("squid:S2077") // since instanceId must be a UUID, it is safe to use inline
    public void dropSchema(UUID instanceId) {
        namedTemplate.getJdbcTemplate().update("drop schema " + quote(instanceId.toString()) + " cascade");
    }

    private UUID safeParseUUID(String input) {
        try {
            return UUID.fromString(input);
        } catch (IllegalArgumentException iae) {
            LOGGER.warn("Found unexpected schema name while listing schemas: [{}]", input);
            return null;
        }
    }

}
