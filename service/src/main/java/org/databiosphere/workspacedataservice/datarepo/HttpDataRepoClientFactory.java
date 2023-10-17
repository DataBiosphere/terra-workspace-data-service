package org.databiosphere.workspacedataservice.datarepo;

import bio.terra.datarepo.api.RepositoryApi;
import bio.terra.datarepo.client.ApiClient;
import javax.ws.rs.client.Client;
import org.apache.commons.lang3.StringUtils;
import org.databiosphere.workspacedataservice.sam.TokenContextUtil;
import org.databiosphere.workspacedataservice.shared.model.BearerToken;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HttpDataRepoClientFactory implements DataRepoClientFactory {

  final String dataRepoUrl;
  private final Client commonHttpClient;
  private static final Logger LOGGER = LoggerFactory.getLogger(HttpDataRepoClientFactory.class);

  public HttpDataRepoClientFactory(String dataRepoUrl) {
    this.dataRepoUrl = dataRepoUrl;
    this.commonHttpClient = new ApiClient().getHttpClient();
  }

  private ApiClient getApiClient() {
    // create a new data repo client
    ApiClient apiClient = new ApiClient();
    apiClient.setHttpClient(commonHttpClient);

    // initialize the client with the url to data repo
    if (StringUtils.isNotBlank(dataRepoUrl)) {
      apiClient.setBasePath(dataRepoUrl);
    }
    // grab the current user's bearer token (see BearerTokenFilter)
    BearerToken token = TokenContextUtil.getToken();
    // add the user's bearer token to the client
    if (token != null) {
      LOGGER.debug("setting access token for data repo request");
      apiClient.setAccessToken(token.value());
    } else {
      LOGGER.warn("No access token found for data repo request.");
    }

    // return the client
    return apiClient;
  }

  public RepositoryApi getRepositoryApi() {
    return new RepositoryApi(getApiClient());
  }
}
