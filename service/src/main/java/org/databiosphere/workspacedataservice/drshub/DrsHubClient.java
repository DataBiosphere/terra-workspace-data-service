package org.databiosphere.workspacedataservice.drshub;

import static org.databiosphere.workspacedataservice.retry.RestClientRetry.RestCall;

import org.databiosphere.workspacedataservice.retry.RestClientRetry;
import org.springframework.web.client.RestClientResponseException;

public class DrsHubClient {

  private final DrsHubApi drsHubApi;
  private final RestClientRetry restClientRetry;

  public DrsHubClient(DrsHubApi drsHubApi, RestClientRetry restClientRetry) {
    this.drsHubApi = drsHubApi;
    this.restClientRetry = restClientRetry;
  }

  public ResourceMetadataResponse resolveDrs(ResolveDrsRequest resolveDrsRequest) {
    try {
      RestCall<ResourceMetadataResponse> restCall = () -> drsHubApi.resolveDrs(resolveDrsRequest);

      return restClientRetry.withRetryAndErrorHandling(restCall, "DrsHub.resolveDrs");
    } catch (RestClientResponseException restException) {
      throw new DrsHubException(restException);
    }
  }
}
