package org.databiosphere.workspacedataservice.rawls;

import io.micrometer.observation.ObservationRegistry;
import java.net.MalformedURLException;
import java.net.URL;
import java.time.Duration;
import org.databiosphere.workspacedataservice.retry.RestClientRetry;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.http.client.ClientHttpRequestFactory;
import org.springframework.http.client.SimpleClientHttpRequestFactory;
import org.springframework.web.client.RestClient;
import org.springframework.web.client.support.RestClientAdapter;
import org.springframework.web.service.invoker.HttpServiceProxyFactory;

@Configuration
public class RawlsClientConfig {

  @Value("${rawlsurl:}")
  private String rawlsUrl;

  @Value("${rawls.connectTimeout:10}")
  private Integer connectTimeout;

  @Value("${rawls.readTimeout:45}")
  private Integer readTimeout;

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

  ClientHttpRequestFactory customRequestFactory() {
    SimpleClientHttpRequestFactory factory = new SimpleClientHttpRequestFactory();
    factory.setReadTimeout(Duration.ofSeconds(readTimeout));
    factory.setConnectTimeout(Duration.ofSeconds(connectTimeout));
    return factory;
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
        .requestFactory(customRequestFactory())
        .build();
  }
}
