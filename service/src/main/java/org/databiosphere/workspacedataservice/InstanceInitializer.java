package org.databiosphere.workspacedataservice;

import org.databiosphere.workspacedataservice.dao.InstanceDao;
import org.databiosphere.workspacedataservice.dao.RecordDao;
import org.databiosphere.workspacedataservice.sam.SamDao;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.ApplicationListener;
import org.springframework.context.annotation.Profile;
import org.springframework.context.event.ContextRefreshedEvent;
import org.springframework.dao.DataAccessException;
import org.springframework.stereotype.Component;

import java.util.UUID;

@Component
@Profile("!local")
public class InstanceInitializer implements
        ApplicationListener<ContextRefreshedEvent> {

    private final SamDao samDao;
    private final InstanceDao instanceDao;

    @Value("${twds.instance.workspace-id}")
    private String workspaceId;

    private static final Logger LOGGER = LoggerFactory.getLogger(InstanceInitializer.class);



    public InstanceInitializer(@Qualifier("AppSamDao") SamDao samDao, InstanceDao instanceDao){
        this.samDao = samDao;
        this.instanceDao = instanceDao;
    }

    @Override
    public void onApplicationEvent(ContextRefreshedEvent event){
        LOGGER.info("Default workspace id loaded as {}", workspaceId);

        try {
            UUID instanceId = UUID.fromString(workspaceId);
            // create `wds-instance` resource in Sam if it doesn't exist
            //TODO how to have this not fail locally and during tests
//            if (!samDao.instanceResourceExists(instanceId)){
//                LOGGER.info("Creating wds-resource for workspaceId {}", workspaceId);
//                //TODO what should the parent id be?
//                samDao.createInstanceResource(instanceId, instanceId);
//            }
            if (!instanceDao.instanceSchemaExists(instanceId)) {
                instanceDao.createSchema(instanceId);
                LOGGER.info("Creating default schema id succeeded for workspaceId {}", workspaceId);
            }
        } catch (IllegalArgumentException e) {
            LOGGER.warn("Workspace id could not be parsed, a default schema won't be created. Provided id: {}", workspaceId);
        } catch (DataAccessException e) {
            LOGGER.error("Failed to create default schema id for workspaceId {}", workspaceId);
        }
        //TODO what errors do i need to catch from sam
    }

}
