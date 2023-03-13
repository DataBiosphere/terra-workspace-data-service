package org.databiosphere.workspacedataservice.service;

import org.broadinstitute.dsde.workbench.client.sam.model.SystemStatus;
import org.databiosphere.workspacedataservice.sam.SamDao;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.actuate.health.AbstractHealthIndicator;
import org.springframework.boot.actuate.health.Health;
import org.springframework.stereotype.Service;

/**
* EDIT THIS
 */
@Service
public class StatusService extends AbstractHealthIndicator {

    private final SamDao samDao;

    private static final Logger LOGGER = LoggerFactory.getLogger(StatusService.class);

    public StatusService(SamDao samDao) {
        this.samDao = samDao;
    }

    @Override
    protected void doHealthCheck(Health.Builder builder) throws Exception {

        SystemStatus samStatus = samDao.getSystemStatus();

        if(samStatus.getOk()) {
            builder.up();
        } else {
            builder.down();
            LOGGER.warn("SAM instance is currently down. Details: {}", samStatus);
        }
    }

}