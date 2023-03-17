package org.databiosphere.workspacedataservice.dao;

import java.util.List;
import java.util.UUID;

public interface InstanceDao {
    boolean instanceSchemaExists(UUID instanceId);

    List<UUID> listInstanceSchemas();

    void createSchema(UUID instanceId);

    void dropSchema(UUID instanceId);

    void createDefaultInstanceSchema(String workspaceId);
}
