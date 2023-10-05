package org.databiosphere.workspacedataservice.dataimport;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.databiosphere.workspacedataservice.dao.JobDao;
import org.databiosphere.workspacedataservice.dao.PostgresJobDao;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;

/** Definitions for job-related beans */
@Configuration
public class JobConfig {

  @Bean
  public JobDao jobDao(NamedParameterJdbcTemplate namedTemplate, ObjectMapper mapper) {
    return new PostgresJobDao(namedTemplate, mapper);
  }
}
