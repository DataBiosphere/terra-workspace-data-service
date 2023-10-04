package org.databiosphere.workspacedataservice.dataimport;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.databiosphere.workspacedataservice.dao.ImportDao;
import org.databiosphere.workspacedataservice.dao.PostgresImportDao;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;

/** Definitions for import-related beans */
@Configuration
public class ImportConfig {

  @Bean
  public ImportDao importDao(NamedParameterJdbcTemplate namedTemplate, ObjectMapper mapper) {
    return new PostgresImportDao(namedTemplate, mapper);
  }
}
