package org.databiosphere.workspacedataservice.sam;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.actuate.health.AbstractHealthIndicator;
import org.springframework.boot.actuate.health.Health;
import org.springframework.stereotype.Service;

/**
* Service that performs health checks on microservices that WDS relies on (such as SAM)
 *
 * See here for more details how we handle health checks in WDS: https://www.baeldung.com/spring-boot-health-indicators
 */
@Service(value = "Permissions")
public class PermissionsStatusService extends AbstractHealthIndicator {

    private final SamDao samDao;

    private static final Logger LOGGER = LoggerFactory.getLogger(PermissionsStatusService.class);

    public PermissionsStatusService(SamDao samDao) {
        this.samDao = samDao;
    }

    @Override
    public void doHealthCheck(Health.Builder builder) {
        builder.up();
        try {
            Boolean samStatus = samDao.getSystemStatusOk();
            builder.withDetail("samOK", samStatus);
        } catch (Exception e) {
            LOGGER.warn("SAM is currently signaled as DOWN.");
            builder.withDetail("samConnectionError", e.getMessage());
        }

    }

}