package org.databiosphere.workspacedataservice.dao;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;
import org.springframework.context.annotation.Profile;

/**
 * @see NoopSchedulerDao
 */
@Configuration
public class NoopSchedulerDaoConfig {
  @Bean
  @Profile("noop-scheduler-dao")
  @Primary
  SchedulerDao noopSchedulerDao() {
    return new NoopSchedulerDao();
  }
}
