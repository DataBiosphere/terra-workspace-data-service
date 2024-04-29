package org.databiosphere.workspacedataservice.dataimport;

import org.databiosphere.workspacedataservice.config.ConfigurationException;
import org.databiosphere.workspacedataservice.config.DataImportProperties;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.env.Environment;

@Configuration
public class ImportValidatorConfiguration {
  @Bean
  @ConditionalOnProperty(
      name = "twds.data-import.require-validation",
      havingValue = "true",
      matchIfMissing = true)
  ImportValidator getDefaultImportValidator(DataImportProperties dataImportProperties) {
    return new DefaultImportValidator(dataImportProperties.getAllowedHosts());
  }

  /** Allow import validation to be disabled for some test workflows. */
  @Bean
  @ConditionalOnProperty(name = "twds.data-import.require-validation", havingValue = "false")
  ImportValidator getNoopImportValidator(Environment environment) {
    if (!environment.matchesProfiles("local")) {
      throw new ConfigurationException("Import validation can only be disabled in local mode.");
    }

    return new NoopImportValidator();
  }
}
