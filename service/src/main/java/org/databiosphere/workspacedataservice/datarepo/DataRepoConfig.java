package org.databiosphere.workspacedataservice.datarepo;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class DataRepoConfig {

  @Value("${datarepourl:}")
  private String dataRepoUrl;

  private static final Logger LOGGER = LoggerFactory.getLogger(DataRepoConfig.class);

  @Bean
  public HttpDataRepoDaoFactory getDataRepoDaoFactory() {
    LOGGER.info("Using data repo base url: '{}'", dataRepoUrl);
    return new HttpDataRepoDaoFactory(dataRepoUrl);
  }
}
