package org.databiosphere.workspacedataservice.dataimport;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.databiosphere.workspacedataservice.dao.JobDao;
import org.databiosphere.workspacedataservice.dao.PostgresJobDao;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;

/** Definitions for import-related beans */
@Configuration
public class ImportConfig {

  @Bean
  public JobDao importDao(NamedParameterJdbcTemplate namedTemplate, ObjectMapper mapper) {
    return new PostgresJobDao(namedTemplate, mapper);
  }
}
