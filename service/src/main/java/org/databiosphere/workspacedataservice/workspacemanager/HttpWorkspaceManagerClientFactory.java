package org.databiosphere.workspacedataservice.workspacemanager;

import bio.terra.workspace.api.ControlledAzureResourceApi;
import bio.terra.workspace.api.ReferencedGcpResourceApi;
import bio.terra.workspace.api.ResourceApi;
import bio.terra.workspace.client.ApiClient;
import javax.ws.rs.client.Client;
import org.apache.commons.lang3.StringUtils;
import org.databiosphere.workspacedataservice.sam.TokenContextUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HttpWorkspaceManagerClientFactory implements WorkspaceManagerClientFactory {

  final String workspaceManagerUrl;
  private final Client commonHttpClient;
  private static final Logger LOGGER =
      LoggerFactory.getLogger(HttpWorkspaceManagerClientFactory.class);

  public HttpWorkspaceManagerClientFactory(String workspaceManagerUrl) {
    this.workspaceManagerUrl = workspaceManagerUrl;
    this.commonHttpClient = new ApiClient().getHttpClient();
  }

  private ApiClient getApiClient(String authToken) {
    // create a new data repo client
    ApiClient apiClient = new ApiClient();
    apiClient.setHttpClient(commonHttpClient);

    // initialize the client with the url to data repo
    if (StringUtils.isNotBlank(workspaceManagerUrl)) {
      apiClient.setBasePath(workspaceManagerUrl);
    }

    // grab the current user's bearer token (see BearerTokenFilter) or use parameter value
    String token = TokenContextUtil.getToken(authToken);

    // add the user's bearer token to the client
    if (token != null) {
      LOGGER.debug("setting access token for workspace manager request");
      apiClient.setAccessToken(token);
    } else {
      LOGGER.warn("No access token found for workspace manager request.");
    }

    // return the client
    return apiClient;
  }

  public ReferencedGcpResourceApi getReferencedGcpResourceApi(String authToken) {
    return new ReferencedGcpResourceApi(getApiClient(authToken));
  }

  public ResourceApi getResourceApi(String authToken) {
    return new ResourceApi(getApiClient(authToken));
  }

  public ControlledAzureResourceApi getAzureResourceApi(String authToken) {
    return new ControlledAzureResourceApi(getApiClient(authToken));
  }
}
