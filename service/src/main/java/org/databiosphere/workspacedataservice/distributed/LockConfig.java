package org.databiosphere.workspacedataservice.distributed;

import javax.sql.DataSource;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.integration.jdbc.lock.DefaultLockRepository;
import org.springframework.integration.jdbc.lock.JdbcLockRegistry;
import org.springframework.integration.jdbc.lock.LockRepository;

@Configuration
public class LockConfig {

  @Bean
  public DefaultLockRepository defaultLockRepository(DataSource dataSource) {
    DefaultLockRepository defaultLockRepository = new DefaultLockRepository(dataSource);
    defaultLockRepository.setPrefix("sys_wds.INT_");
    defaultLockRepository.setTimeToLive(10 * 60 * 1000); // 10 minutes
    return defaultLockRepository;
  }

  @Bean
  public JdbcLockRegistry jdbcLockRegistry(LockRepository lockRepository) {
    return new JdbcLockRegistry(lockRepository);
  }
}
