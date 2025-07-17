package org.databiosphere.workspacedataservice.rawls;

import io.micrometer.observation.ObservationRegistry;
import java.net.MalformedURLException;
import java.net.URL;
import org.databiosphere.workspacedataservice.retry.RestClientRetry;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.web.client.RestClient;
import org.springframework.web.client.support.RestClientAdapter;
import org.springframework.web.service.invoker.HttpServiceProxyFactory;

@Configuration
public class RawlsClientConfig {

  @Value("${rawlsurl:}")
  private String rawlsUrl;

  @Bean
  public RawlsClient rawlsClient(RawlsApi rawlsApi, RestClientRetry restClientRetry) {
    return new RawlsClient(rawlsApi, restClientRetry);
  }

  // RestClient-enabled proxy for the Rawls API
  @Bean
  public RawlsApi rawlsApi(@Qualifier("rawlsRestClient") RestClient restClient) {
    HttpServiceProxyFactory httpServiceProxyFactory =
        HttpServiceProxyFactory.builderFor(RestClientAdapter.create(restClient)).build();

    return httpServiceProxyFactory.createClient(RawlsApi.class);
  }

  // fluent RestClient, initialized with Rawls' base url, auth from TokenContextUtil, and the
  // current observationRegistry for Prometheus metrics
  @Bean
  public RestClient rawlsRestClient(ObservationRegistry observationRegistry)
      throws MalformedURLException {

    // validate the Rawls url is well-formed.
    // this will throw and prevent Spring startup if the Rawls url is invalid.
    new URL(rawlsUrl);

    return RestClient.builder()
        .observationRegistry(observationRegistry)
        .baseUrl(rawlsUrl)
        .requestInitializer(new BearerAuthRequestInitializer())
        .build();
  }
}
