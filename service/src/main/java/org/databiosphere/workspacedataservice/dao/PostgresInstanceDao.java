package org.databiosphere.workspacedataservice.dao;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.dao.DataAccessException;
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

    public PostgresInstanceDao(NamedParameterJdbcTemplate namedTemplate, @Value("${twds.instance.workspace-id}") String workspaceId) {
        this.namedTemplate = namedTemplate;
        createDefaultInstanceSchema(workspaceId);
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
    public void createSchema(UUID instanceId) {
        namedTemplate.getJdbcTemplate().update("create schema " + quote(instanceId.toString()));
    }


    @Override
    @SuppressWarnings("squid:S2077") // since instanceId must be a UUID, it is safe to use inline
    public void dropSchema(UUID instanceId) {
        namedTemplate.getJdbcTemplate().update("drop schema " + quote(instanceId.toString()) + " cascade");
    }


    private void createDefaultInstanceSchema(String workspaceId) {
        LOGGER.info("Default workspace id loaded as {}", workspaceId);

        // TODO: AJ-897 execute this as the WDS managed identity so it can call Sam
        // TODO: AJ-897 move to a dedicated StartupBean

        try {
            UUID instanceId = UUID.fromString(workspaceId);
            if (!instanceSchemaExists(instanceId)) {
                createSchema(instanceId);
                LOGGER.info("Creating default schema id succeeded for workspaceId {}", workspaceId);
            }
        } catch (IllegalArgumentException e) {
            LOGGER.warn("Workspace id could not be parsed, a default schema won't be created. Provided id: {}", workspaceId);
        } catch (DataAccessException e) {
            LOGGER.error("Failed to create default schema id for workspaceId {}", workspaceId);
        }
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
