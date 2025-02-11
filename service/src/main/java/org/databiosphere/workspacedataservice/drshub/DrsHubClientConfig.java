package org.databiosphere.workspacedataservice.drshub;

import io.micrometer.observation.ObservationRegistry;
import java.net.MalformedURLException;
import java.net.URL;
import org.databiosphere.workspacedataservice.rawls.BearerAuthRequestInitializer;
import org.databiosphere.workspacedataservice.retry.RestClientRetry;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.web.client.RestClient;
import org.springframework.web.client.support.RestClientAdapter;
import org.springframework.web.service.invoker.HttpServiceProxyFactory;

@Configuration
public class DrsHubClientConfig {

  @Value("${drshuburl:}")
  private String drsHubUrl;

  @Bean
  public DrsHubClient drsHubClient(DrsHubApi drsHubApi, RestClientRetry restClientRetry) {
    return new DrsHubClient(drsHubApi, restClientRetry);
  }

  @Bean
  public DrsHubApi drsHubApi(@Qualifier("drsHubRestClient") RestClient restClient) {
    HttpServiceProxyFactory httpServiceProxyFactory =
        HttpServiceProxyFactory.builderFor(RestClientAdapter.create(restClient)).build();

    return httpServiceProxyFactory.createClient(DrsHubApi.class);
  }

  @Bean
  public RestClient drsHubRestClient(ObservationRegistry observationRegistry)
      throws MalformedURLException {

    new URL(drsHubUrl); // validate the DRS Hub URL is well-formed.

    return RestClient.builder()
        .observationRegistry(observationRegistry)
        .baseUrl(drsHubUrl)
        .requestInitializer(new BearerAuthRequestInitializer())
        .build();
  }
}
