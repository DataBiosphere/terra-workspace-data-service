package org.databiosphere.workspacedataservice.sam;

import java.util.List;
import okhttp3.OkHttpClient;
import okhttp3.Protocol;
import org.apache.commons.lang3.StringUtils;
import org.broadinstitute.dsde.workbench.client.sam.ApiClient;
import org.broadinstitute.dsde.workbench.client.sam.api.GoogleApi;
import org.broadinstitute.dsde.workbench.client.sam.api.ResourcesApi;
import org.broadinstitute.dsde.workbench.client.sam.api.StatusApi;
import org.broadinstitute.dsde.workbench.client.sam.api.UsersApi;
import org.databiosphere.workspacedataservice.shared.model.BearerToken;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Implementation of SamClientFactory that creates a Sam ApiClient, initializes that client with the
 * url to Sam, adds the current user's access token to the client, and then returns the ResourcesApi
 * from that client. ResourcesApi is the part of the Sam client used by WDS.
 */
public class HttpSamClientFactory implements SamClientFactory {

  private final String samUrl;
  private final OkHttpClient commonHttpClient;

  private static final Logger LOGGER = LoggerFactory.getLogger(HttpSamClientFactory.class);

  public HttpSamClientFactory(String samUrl) {
    this.samUrl = samUrl;
    // IntelliJ has a false-positive error on the following line; see
    // https://youtrack.jetbrains.com/issue/KTIJ-26434
    this.commonHttpClient =
        new ApiClient().getHttpClient().newBuilder().protocols(List.of(Protocol.HTTP_1_1)).build();
    // TODO AJ-1114: add tracing interceptor for distributed tracing to Sam.
    // this requires we import terra-common-lib
  }

  private ApiClient getApiClient(BearerToken authToken) {
    // create a new Sam client
    ApiClient apiClient = new ApiClient();
    apiClient.setHttpClient(commonHttpClient);
    // initialize the client with the url to Sam
    if (StringUtils.isNotBlank(samUrl)) {
      apiClient.setBasePath(samUrl);
    }

    // use the parameter value or fall back to the current user's bearer token (see
    // BearerTokenFilter)
    BearerToken token = TokenContextUtil.getToken(authToken);

    // add the user's bearer token to the client
    if (token.nonEmpty()) {
      LOGGER.debug("setting access token for Sam request");
      apiClient.setAccessToken(token.getValue());
    } else {
      LOGGER.warn("No access token found for Sam request.");
    }

    // return the client
    return apiClient;
  }

  /**
   * Get a ResourcesApi Sam client, initialized with the url to Sam and the current user's access
   * token, if any
   *
   * @return the usable Sam client
   */
  public ResourcesApi getResourcesApi() {
    ApiClient apiClient = getApiClient(BearerToken.empty());
    ResourcesApi resourcesApi = new ResourcesApi();
    resourcesApi.setApiClient(apiClient);
    return resourcesApi;
  }

  /**
   * Get a StatusApi Sam client, initialized with the url to Sam and the current user's access
   * token, if any
   *
   * @return the usable Sam client
   */
  public StatusApi getStatusApi(BearerToken token) {
    ApiClient apiClient = getApiClient(token);
    StatusApi statusApi = new StatusApi();
    statusApi.setApiClient(apiClient);
    return statusApi;
  }

  public UsersApi getUsersApi(BearerToken token) {
    ApiClient apiClient = getApiClient(token);
    UsersApi usersApi = new UsersApi();
    usersApi.setApiClient(apiClient);
    return usersApi;
  }

  public GoogleApi getGoogleApi(BearerToken token) {
    ApiClient apiClient = getApiClient(token);
    GoogleApi googleApi = new GoogleApi();
    googleApi.setApiClient(apiClient);
    return googleApi;
  }
}
