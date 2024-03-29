package org.databiosphere.workspacedataservice.leonardo;

import java.util.List;
import okhttp3.OkHttpClient;
import okhttp3.Protocol;
import org.apache.commons.lang3.StringUtils;
import org.broadinstitute.dsde.workbench.client.leonardo.ApiClient;
import org.broadinstitute.dsde.workbench.client.leonardo.api.AppsApi;
import org.databiosphere.workspacedataservice.sam.TokenContextUtil;
import org.databiosphere.workspacedataservice.shared.model.BearerToken;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HttpLeonardoClientFactory implements LeonardoClientFactory {

  final String leoEndpointUrl;
  private final OkHttpClient commonHttpClient;
  private static final Logger LOGGER = LoggerFactory.getLogger(HttpLeonardoClientFactory.class);

  public HttpLeonardoClientFactory(String leoUrl) {
    this.leoEndpointUrl = leoUrl;
    // IntelliJ has a false-positive error on the following line; see
    // https://youtrack.jetbrains.com/issue/KTIJ-26434
    this.commonHttpClient =
        new ApiClient().getHttpClient().newBuilder().protocols(List.of(Protocol.HTTP_1_1)).build();
  }

  private ApiClient getApiClient(String authToken) {
    // create a new data repo client
    ApiClient apiClient = new ApiClient();
    apiClient.setHttpClient(commonHttpClient);

    // initialize the client with the url to leo endpoint
    if (StringUtils.isNotBlank(leoEndpointUrl)) {
      apiClient.setBasePath(leoEndpointUrl);
    }

    // grab the current user's bearer token (see BearerTokenFilter) or use parameter value
    BearerToken token = TokenContextUtil.getToken(authToken);
    // add the user's bearer token to the client
    if (token.nonEmpty()) {
      LOGGER.debug("setting access token for leonardo request");
      apiClient.setAccessToken(token.getValue());
    } else {
      LOGGER.warn("No access token found for leonardo request.");
    }

    // return the client
    return apiClient;
  }

  public AppsApi getAppsV2Api(String token) {
    return new AppsApi(getApiClient(token));
  }
}
