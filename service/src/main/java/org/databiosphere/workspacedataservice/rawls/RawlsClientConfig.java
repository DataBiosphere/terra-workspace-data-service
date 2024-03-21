package org.databiosphere.workspacedataservice.rawls;

import static org.databiosphere.workspacedataservice.annotations.DeploymentMode.*;

import io.micrometer.observation.ObservationRegistry;
import org.databiosphere.workspacedataservice.retry.RestClientRetry;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.web.client.RestClient;
import org.springframework.web.client.support.RestClientAdapter;
import org.springframework.web.service.invoker.HttpServiceProxyFactory;

@Configuration
@ControlPlane
public class RawlsClientConfig {

  @Value("${rawlsurl:}")
  private String rawlsUrl;

  @Bean
  @ControlPlane
  public RawlsClient rawlsClient(RawlsApi rawlsApi, RestClientRetry restClientRetry) {
    return new RawlsClient(rawlsApi, restClientRetry);
  }

  // RestClient-enabled proxy for the Rawls API
  @Bean
  @ControlPlane
  public RawlsApi rawlsApi(RestClient restClient) {
    HttpServiceProxyFactory httpServiceProxyFactory =
        HttpServiceProxyFactory.builderFor(RestClientAdapter.create(restClient)).build();

    return httpServiceProxyFactory.createClient(RawlsApi.class);
  }

  // fluent RestClient, initialized with Rawls' base url, auth from TokenContextUtil, and the
  // current observationRegistry for Prometheus metrics
  @Bean
  @ControlPlane
  public RestClient rawlsRestClient(ObservationRegistry observationRegistry) {
    return RestClient.builder()
        .observationRegistry(observationRegistry)
        .baseUrl(rawlsUrl)
        .requestInitializer(new BearerAuthRequestInitializer())
        .build();
  }
}
