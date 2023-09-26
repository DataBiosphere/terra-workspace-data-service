package org.databiosphere.workspacedataservice.dao;

import javax.sql.DataSource;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.jdbc.DataSourceBuilder;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;
import org.springframework.integration.jdbc.lock.DefaultLockRepository;
import org.springframework.integration.jdbc.lock.JdbcLockRegistry;
import org.springframework.integration.jdbc.lock.LockRepository;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;

@Configuration
public class DataSourceConfig {

  @Bean
  @Primary
  @ConfigurationProperties("spring.datasource.hikari")
  public DataSource mainDb() {
    return DataSourceBuilder.create().build();
  }

  @Bean
  @Primary
  public NamedParameterJdbcTemplate namedParameterJdbcTemplate(DataSource dataSource) {
    return new NamedParameterJdbcTemplate(dataSource);
  }

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
