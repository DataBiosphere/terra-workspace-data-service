package org.databiosphere.workspacedataservice.dataimport;

import org.databiosphere.workspacedataservice.config.DataImportProperties;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;

@Configuration
public class ImportValidatorConfiguration {
  @Bean
  @ConditionalOnProperty(
      name = "twds.data-import.disable-validation",
      havingValue = "false",
      matchIfMissing = true)
  ImportValidator getDefaultImportValidator(DataImportProperties dataImportProperties) {
    return new DefaultImportValidator(dataImportProperties.getAllowedHosts());
  }

  @Bean
  @ConditionalOnProperty(name = "twds.data-import.disable-validation", havingValue = "true")
  @Profile("local")
  ImportValidator getNoopImportValidator() {
    return new NoopImportValidator();
  }
}
