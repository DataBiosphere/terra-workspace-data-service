package org.databiosphere.workspacedataservice.dao;

import org.postgresql.util.ServerErrorMessage;

import java.sql.SQLException;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Mock implementation of InstanceDao that is in-memory instead of requiring Postgres
 */
public class MockInstanceDao implements InstanceDao {

    // backing "database" for this mock
    private Set<UUID> instances = ConcurrentHashMap.newKeySet();

    public MockInstanceDao() {
        super();
    }


    @Override
    public boolean instanceSchemaExists(UUID instanceId) {
        return instances.contains(instanceId);
    }

    @Override
    public List<UUID> listInstanceSchemas() {
        return instances.stream().toList();
    }

    @Override
    public void createSchema(UUID instanceId) {
        if (instances.contains(instanceId)) {
            ServerErrorMessage sqlMsg = new ServerErrorMessage("ERROR: schema \"" + instanceId.toString() + "\" already exists");
            SQLException ex = new org.postgresql.util.PSQLException(sqlMsg);
            String sql = "create schema \"" + instanceId + "\"";
            throw new org.springframework.jdbc.BadSqlGrammarException("StatementCallback", sql, ex);
        }
        instances.add(instanceId);
    }

    @Override
    public void dropSchema(UUID instanceId) {
        if (!instances.contains(instanceId)) {
            ServerErrorMessage sqlMsg = new ServerErrorMessage("ERROR: schema \"" + instanceId.toString() + "\" does not exist");
            SQLException ex = new org.postgresql.util.PSQLException(sqlMsg);
            String sql = "drop schema \"" + instanceId + "\" cascade";
            throw new org.springframework.jdbc.BadSqlGrammarException("StatementCallback", sql, ex);
        }
        instances.remove(instanceId);
    }

}
