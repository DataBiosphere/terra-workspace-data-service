package org.databiosphere.workspacedataservice.rawls;

import static org.databiosphere.workspacedataservice.annotations.DeploymentMode.*;

import org.databiosphere.workspacedataservice.retry.RestClientRetry;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.web.client.RestTemplateBuilder;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.web.client.RestTemplate;

@Configuration
@ControlPlane
public class RawlsClientConfig {

  @Value("${rawlsurl:}")
  private String rawlsUrl;

  @Bean
  @ControlPlane
  public RawlsClient rawlsClient(RestTemplate restTemplate, RestClientRetry restClientRetry) {
    return new RawlsClient(rawlsUrl, restTemplate, restClientRetry);
  }

  @Bean
  @ControlPlane
  public RestTemplate restTemplate(RestTemplateBuilder builder) {
    return builder.build();
  }
}
