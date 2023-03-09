package org.databiosphere.workspacedataservice.health;

import org.springframework.boot.actuate.health.AbstractHealthIndicator;
import org.springframework.boot.actuate.health.Health;
import org.springframework.stereotype.Component;

// TODO

@Component
public class SamHealthCheck extends AbstractHealthIndicator {
    @Override
    protected void doHealthCheck(Health.Builder bldr) throws Exception {

    }
}