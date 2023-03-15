package org.databiosphere.workspacedataservice.sam;

import org.broadinstitute.dsde.workbench.client.sam.model.SystemStatus;
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
@Service(value = "Sam")
public class SamStatusService extends AbstractHealthIndicator {

    private final SamDao samDao;

    private static final Logger LOGGER = LoggerFactory.getLogger(SamStatusService.class);

    public SamStatusService(SamDao samDao) {
        this.samDao = samDao;
    }

    @Override
    public void doHealthCheck(Health.Builder builder) {
        // we don't want a problem with the Sam connection to take WDS down entirely. So,
        // we always call builder.up() here, but we include the actual Sam status in the builder detail.
        builder.up();

        try {
            SystemStatus samStatus = samDao.getSystemStatus();
            builder.withDetail("ok", samStatus.getOk());
        } catch (Exception e) {
            // SAM "status" will still return "UP"; however, details.status should be evaluated instead to determine the health of Sam
            builder.withDetail("status", "DOWN");
            builder.withDetail("connectionError", e.getMessage());
        }
    }

}