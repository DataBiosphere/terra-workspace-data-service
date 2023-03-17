package org.databiosphere.workspacedataservice;

import org.databiosphere.workspacedataservice.dao.InstanceDao;
import org.databiosphere.workspacedataservice.dao.RecordDao;
import org.databiosphere.workspacedataservice.sam.SamDao;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.ApplicationListener;
import org.springframework.context.event.ContextRefreshedEvent;
import org.springframework.stereotype.Component;

import java.util.UUID;

@Component
public class InstanceInitializer implements
        ApplicationListener<ContextRefreshedEvent> {

    private final SamDao samDao;
    private final InstanceDao instanceDao;

    @Value("${twds.instance.workspace-id}")
    private String instanceId;

    private static final Logger LOGGER = LoggerFactory.getLogger(InstanceInitializer.class);



    public InstanceInitializer(@Qualifier("AppSamDao") SamDao samDao, InstanceDao instanceDao){
        this.samDao = samDao;
        this.instanceDao = instanceDao;
    }

    @Override
    public void onApplicationEvent(ContextRefreshedEvent event){
        // create `wds-instance` resource in Sam if it doesn't exist
        if (!samDao.instanceResourceExists(instanceId)){
            LOGGER.info("wds-resource does not exist yet");
            samDao.createInstanceResource(instanceId, instanceId);
        }
        instanceDao.createDefaultInstanceSchema(instanceId);
    }

}
