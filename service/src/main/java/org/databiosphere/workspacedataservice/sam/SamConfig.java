package org.databiosphere.workspacedataservice.sam;

import java.util.UUID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * Bean creator for: - SamClientFactory, injecting the base url to Sam into that factory. - SamDao,
 * injecting the SamClientFactory into that dao.
 */
@Configuration
public class SamConfig {

  @Value("${SAM_URL:}")
  private String samUrl;

  @Value("${twds.instance.workspace-id:}")
  private String workspaceIdArgument;

  private static final Logger LOGGER = LoggerFactory.getLogger(SamConfig.class);

  @Bean
  HttpSamClientSupport getHttpSamClientSupport() {
    return new HttpSamClientSupport();
  }

  @Bean
  public SamClientFactory getSamClientFactory() {
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
  public SamDao samDao(
      SamClientFactory samClientFactory, HttpSamClientSupport httpSamClientSupport) {
    // Try to parse the WORKSPACE_ID env var;
    // return a MisconfiguredSamDao if it can't be parsed.
    try {
      String workspaceId = UUID.fromString(workspaceIdArgument).toString(); // verify UUID-ness
      LOGGER.info(
          "Sam integration will query type={}, resourceId={}, action={}",
          SamDao.RESOURCE_NAME_WORKSPACE,
          workspaceId,
          SamDao.ACTION_WRITE);
      return new HttpSamDao(samClientFactory, httpSamClientSupport, workspaceId);
    } catch (IllegalArgumentException e) {
      LOGGER.warn(
          "Workspace id could not be parsed, all Sam permission checks will fail. Provided id: {}",
          workspaceIdArgument);
      return new MisconfiguredSamDao(
          "WDS was started with invalid WORKSPACE_ID of: " + workspaceIdArgument);
    } catch (Exception e) {
      LOGGER.warn("Error during initial Sam configuration: " + e.getMessage());
      return new MisconfiguredSamDao(e.getMessage());
    }
  }
}
