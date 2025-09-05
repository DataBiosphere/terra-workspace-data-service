package org.databiosphere.workspacedataservice.dataimport;

import org.databiosphere.workspacedataservice.config.ConfigurationException;
import org.databiosphere.workspacedataservice.config.DataImportProperties;
import org.databiosphere.workspacedataservice.config.DrsImportProperties;
import org.databiosphere.workspacedataservice.dataimport.protecteddatasupport.ProtectedDataSupport;
import org.databiosphere.workspacedataservice.sam.SamDao;
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
  ImportValidator getDefaultImportValidator(
      ProtectedDataSupport protectedDataSupport,
      SamDao samDao,
      DataImportProperties dataImportProperties,
      ConnectivityChecker connectivityChecker,
      DrsImportProperties drsImportProperties) {
    return new DefaultImportValidator(
        protectedDataSupport,
        samDao,
        dataImportProperties.getAllowedHosts(),
        dataImportProperties.getSources(),
        dataImportProperties.getRawlsBucketName(),
        connectivityChecker,
        drsImportProperties,
        dataImportProperties.getAllowedBuckets());
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
