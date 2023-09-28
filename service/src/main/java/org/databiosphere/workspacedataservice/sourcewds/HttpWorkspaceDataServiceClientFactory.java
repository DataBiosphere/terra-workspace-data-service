package org.databiosphere.workspacedataservice.sourcewds;

import static org.databiosphere.workspacedataservice.sam.BearerTokenFilter.ATTRIBUTE_NAME_TOKEN;
import static org.springframework.web.context.request.RequestAttributes.SCOPE_REQUEST;

import java.util.List;
import java.util.Objects;
import okhttp3.OkHttpClient;
import okhttp3.Protocol;
import org.apache.commons.lang3.StringUtils;
import org.databiosphere.workspacedata.api.CloningApi;
import org.databiosphere.workspacedata.client.ApiClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.web.context.request.RequestContextHolder;

public class HttpWorkspaceDataServiceClientFactory implements WorkspaceDataServiceClientFactory {
  private final OkHttpClient commonHttpClient;
  private static final Logger LOGGER =
      LoggerFactory.getLogger(HttpWorkspaceDataServiceClientFactory.class);

  public HttpWorkspaceDataServiceClientFactory() {
    // IntelliJ has a false-positive error on the following line; see
    // https://youtrack.jetbrains.com/issue/KTIJ-26434
    this.commonHttpClient =
        new ApiClient().getHttpClient().newBuilder().protocols(List.of(Protocol.HTTP_1_1)).build();
    ;
  }

  private ApiClient getApiClient(String token, String workspaceDataServiceUrl) {
    // create a new client
    ApiClient apiClient = new ApiClient();
    apiClient.setHttpClient(commonHttpClient);

    // initialize the client with the url to wds endpoint
    if (StringUtils.isNotBlank(workspaceDataServiceUrl)) {
      LOGGER.info("Setting Wds endpoint url to: {}", workspaceDataServiceUrl);
      apiClient.setBasePath(workspaceDataServiceUrl);
    }

    // grab the current user's bearer token (see BearerTokenFilter)
    if (token.isEmpty()) {
      Object userToken =
          RequestContextHolder.currentRequestAttributes()
              .getAttribute(ATTRIBUTE_NAME_TOKEN, SCOPE_REQUEST);
      // add the user's bearer token to the client
      if (!Objects.isNull(userToken)) {
        LOGGER.debug("setting access token for workspace data service request");
        apiClient.setAccessToken(userToken.toString());
      } else {
        LOGGER.warn("No access token found for workspace data service request.");
      }
    } else {
      apiClient.setBearerToken(token);
    }

    return apiClient;
  }

  public CloningApi getBackupClient(String token, String wdsUrl) {
    return new CloningApi(getApiClient(token, wdsUrl));
  }
}
