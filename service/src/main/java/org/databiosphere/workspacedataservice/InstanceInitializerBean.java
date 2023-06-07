package org.databiosphere.workspacedataservice;

import org.apache.commons.lang3.StringUtils;
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

    @Value("${twds.instance.source-workspace-id}")
    private String sourceWorkspaceId;

    /*
        currently unused; future code will use this token to:
            - ask WSM about the workspace's storage container
            - retrieve a SAS token for that container from WSM
            - kick off a backup operation in the source WDS
     */
    @Value("${twds.startup-token}")
    private String startupToken;

    private static final Logger LOGGER = LoggerFactory.getLogger(InstanceInitializerBean.class);

    public InstanceInitializerBean(InstanceDao instanceDao){
        this.instanceDao = instanceDao;
    }

    public boolean isInCloneMode(String sourceWorkspaceId) {
        if (StringUtils.isNotBlank(sourceWorkspaceId)){
            LOGGER.info("Source workspace id found, checking database");
            try {
                UUID.fromString(sourceWorkspaceId);
            } catch (IllegalArgumentException e){
                    LOGGER.warn("Source workspace id could not be parsed, unable to clone DB. Provided source workspace id: {}", sourceWorkspaceId);
                    return false;
            }
            try {
                //TODO: this is a placeholder for checking that the db has already been cloned;
                // like in case if wds restarts later on or fails during cloning start up but back up is already there/etc
                //In the future it could check the clone status,
                //But for now assuming if we've created a workspace schema, work is done
                return !instanceDao.instanceSchemaExists(UUID.fromString(workspaceId));
            } catch (IllegalArgumentException e) {
                LOGGER.warn("Workspace id could not be parsed, unable to clone DB. Provided default workspace id: {}", workspaceId);
                return false;
            }
        }
        LOGGER.info("No source workspace id found, initializing default schema");
        return false;

    }

    public void initCloneMode(){
        LOGGER.info("Starting in clone mode...");

        // trigger back up
    }

    public void initializeInstance() {
        LOGGER.info("Default workspace id loaded as {}", workspaceId);
        if (isInCloneMode(sourceWorkspaceId)) {
            LOGGER.info("Source workspace id loaded as {}", sourceWorkspaceId);
            initCloneMode();
        }
        else
            initializeDefaultInstance(); //TODO Wrap this in an else once cloning is implemented
    }

    public void initializeDefaultInstance() {

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
