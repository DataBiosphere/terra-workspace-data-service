package org.databiosphere.workspacedataservice.activitylog;

import org.databiosphere.workspacedataservice.sam.SamDao;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class ActivityLoggerConfig {

    @Bean
    public ActivityLogger getActivityLogger(SamDao samDao) {
        return new ActivityLogger(samDao);
    }

}
