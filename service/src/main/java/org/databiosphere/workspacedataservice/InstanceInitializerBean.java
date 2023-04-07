package org.databiosphere.workspacedataservice;

import org.databiosphere.workspacedataservice.dao.InstanceDao;
import org.databiosphere.workspacedataservice.dao.ManagedIdentityDao;
import org.databiosphere.workspacedataservice.sam.SamDao;
import org.databiosphere.workspacedataservice.service.model.exception.SamException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.dao.DataAccessException;

import java.util.UUID;

public class InstanceInitializerBean {

    private final SamDao samDao;
    private final InstanceDao instanceDao;
    private final ManagedIdentityDao managedIdentityDao;

    @Value("${twds.instance.workspace-id}")
    private String workspaceId;

    @Value("${twds.instance.source-workspace-id}")
    private String sourceWorkspaceId;

    private static final Logger LOGGER = LoggerFactory.getLogger(InstanceInitializerBean.class);

    public InstanceInitializerBean(SamDao samDao, InstanceDao instanceDao, ManagedIdentityDao managedIdentityDao){
        this.samDao = samDao;
        this.instanceDao = instanceDao;
        this.managedIdentityDao = managedIdentityDao;
    }

    public boolean isInClodeMode() {
        if (sourceWorkspaceId != null && !"".equals(sourceWorkspaceId)){
            LOGGER.info("Source workspace id found, checking database");
            try {
                //TODO: this is a placeholder for checking that the db has already been cloned;
                //In the future it could check the clone status,
                //But for now assuming if we've created a workspace schema, work is done
                return !instanceDao.instanceSchemaExists(UUID.fromString(workspaceId));
            } catch (IllegalArgumentException e) {
                LOGGER.warn("Workspace id could not be parsed, unable to clone DB. Provided id: {}", workspaceId);
                //by default this will continue and return false, thereby creating a default schema - do we want to do that?
                //If we return true instead, we'll encounter errors down the line...
            }
        }
        return false;

    }

    public void initCloneMode(){
        LOGGER.info("Beginning clone...");
    }

    public void initializeInstance() {
        LOGGER.info("Default workspace id loaded as {}", workspaceId);
        LOGGER.info("Source workspace id loaded as {}", sourceWorkspaceId);
        if (isInClodeMode())
            initCloneMode();
        else {
            initializeDefaultInstance();
        }
    }

    public void initializeDefaultInstance() {

        try {
            UUID instanceId = UUID.fromString(workspaceId);
            String token = managedIdentityDao.getAzureCredential();
            if (token != null){
                // create `wds-instance` resource in Sam if it doesn't exist
                if (!samDao.instanceResourceExists(instanceId, token)){
                    LOGGER.info("Creating wds-resource for workspaceId {}", workspaceId);
                    samDao.createInstanceResource(instanceId, instanceId, token);
                } else {
                    LOGGER.debug("wds-resource for workspaceId {} already exists; skipping creation.", workspaceId);
                }
                if (!instanceDao.instanceSchemaExists(instanceId)) {
                    instanceDao.createSchema(instanceId);
                    LOGGER.info("Creating default schema id succeeded for workspaceId {}", workspaceId);
                } else {
                    LOGGER.debug("Default schema for workspaceId {} already exists; skipping creation.", workspaceId);
                }
            } else {
                LOGGER.warn("No token acquired from azure managed identity; wds-instance resource and default schema not created");
            }

        } catch (IllegalArgumentException e) {
            LOGGER.warn("Workspace id could not be parsed, a default schema won't be created. Provided id: {}", workspaceId);
        } catch (
        DataAccessException e) {
            LOGGER.error("Failed to create default schema id for workspaceId {}", workspaceId);
        } catch (SamException e) {
            LOGGER.error("Exception thrown from sam, wds-instance resource and default schema not created : {}", e.getMessage());
        }
    }
}
