package org.databiosphere.workspacedataservice;

import org.databiosphere.workspacedataservice.dao.InstanceDao;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.dao.DataAccessException;

import java.util.UUID;

public class InstanceInitializerBean {

    private final InstanceDao instanceDao;

    @Value("${twds.instance.workspace-id}")
    private String workspaceId;

    private static final Logger LOGGER = LoggerFactory.getLogger(InstanceInitializerBean.class);

    public InstanceInitializerBean(InstanceDao instanceDao){
        this.instanceDao = instanceDao;
    }

    public void initializeInstance() {
        LOGGER.info("Default workspace id loaded as {}", workspaceId);

        try {
            UUID instanceId = UUID.fromString(workspaceId);

            if (!instanceDao.instanceSchemaExists(instanceId)) {
                instanceDao.createSchema(instanceId);
                LOGGER.info("Creating default schema id succeeded for workspaceId {}", workspaceId);
            } else {
                LOGGER.debug("Default schema for workspaceId {} already exists; skipping creation.", workspaceId);
            }

        } catch (IllegalArgumentException e) {
            LOGGER.warn("Workspace id could not be parsed, a default schema won't be created. Provided id: {}", workspaceId);
        } catch (DataAccessException e) {
            LOGGER.error("Failed to create default schema id for workspaceId {}", workspaceId);
        }
    }
}
