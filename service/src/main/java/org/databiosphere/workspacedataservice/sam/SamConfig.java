package org.databiosphere.workspacedataservice.sam;

import org.databiosphere.workspacedataservice.retry.RestClientRetry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * Bean creator for:
 *
 * <ul>
 *   <li>{@link SamClientFactory}, injecting the base url to Sam.
 *   <li>{@link SamDao} and {@link SamAuthorizationDaoFactory}, injecting the SamClientFactory and
 *       {@link RestClientRetry}.
 * </ul>
 */
@Configuration
public class SamConfig {

  @Value("${samurl:}")
  private String samUrl;

  private static final Logger LOGGER = LoggerFactory.getLogger(SamConfig.class);

  @Bean
  SamClientFactory getSamClientFactory() {
    // TODO: AJ-898 what validation of the sam url should we do here?
    // - none
    // - check if the value is null/empty/whitespace
    // - check if the value is a valid Url
    // - contact the url and see if it looks like Sam on the other end
    // TODO: AJ-898 and what should we do if the validation fails?
    // - nothing, which would almost certainly result in Sam calls failing
    // - disable Sam integration, which could result in unauthorized access
    // - stop WDS, which would obviously prevent WDS from working at all
    LOGGER.info("Using Sam base url: '{}'", samUrl);
    return new HttpSamClientFactory(samUrl);
  }

  @Bean
  SamAuthorizationDaoFactory samAuthorizationDaoFactory(
      SamClientFactory samClientFactory, RestClientRetry restClientRetry) {
    return new SamAuthorizationDaoFactory(samClientFactory, restClientRetry);
  }

  @Bean
  public SamDao samDao(SamClientFactory samClientFactory, RestClientRetry restClientRetry) {
    return new HttpSamDao(samClientFactory, restClientRetry);
  }
}
