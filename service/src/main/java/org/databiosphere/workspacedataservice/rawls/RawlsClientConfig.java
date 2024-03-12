package org.databiosphere.workspacedataservice.rawls;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.web.client.RestTemplateBuilder;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.web.client.RestTemplate;

@Configuration
public class RawlsClientConfig {

  @Value("${rawlsurl:}")
  private String rawlsUrl;

  @Bean
  public RawlsClient rawlsClient(RestTemplate restTemplate) {
    return new RawlsClient(rawlsUrl, restTemplate);
  }

  @Bean
  public RestTemplate restTemplate(RestTemplateBuilder builder) {
    return builder.build();
  }
}
