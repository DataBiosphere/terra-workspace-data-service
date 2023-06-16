package org.databiosphere.workspacedataservice.workspacemanager;

import bio.terra.workspace.api.ReferencedGcpResourceApi;
import bio.terra.workspace.api.ControlledAzureResourceApi;
import bio.terra.workspace.api.ResourceApi;
import bio.terra.workspace.client.ApiClient;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.web.context.request.RequestContextHolder;

import javax.ws.rs.client.Client;
import java.util.Objects;

import static org.databiosphere.workspacedataservice.sam.BearerTokenFilter.ATTRIBUTE_NAME_TOKEN;
import static org.springframework.web.context.request.RequestAttributes.SCOPE_REQUEST;

public class HttpWorkspaceManagerClientFactory implements WorkspaceManagerClientFactory {

    final String workspaceManagerUrl;
    private final Client commonHttpClient;
    private static final Logger LOGGER = LoggerFactory.getLogger(HttpWorkspaceManagerClientFactory.class);


    public HttpWorkspaceManagerClientFactory(String workspaceManagerUrl) {
        this.workspaceManagerUrl = workspaceManagerUrl;
        this.commonHttpClient = new ApiClient().getHttpClient();
    }

    private ApiClient getApiClient() {
        // create a new data repo client
        ApiClient apiClient = new ApiClient();
        apiClient.setHttpClient(commonHttpClient);

        // initialize the client with the url to data repo
        if (StringUtils.isNotBlank(workspaceManagerUrl)) {
            apiClient.setBasePath(workspaceManagerUrl);
        }
        // grab the current user's bearer token (see BearerTokenFilter)
        Object token = RequestContextHolder.currentRequestAttributes()
                .getAttribute(ATTRIBUTE_NAME_TOKEN, SCOPE_REQUEST);
        // add the user's bearer token to the client
        if (!Objects.isNull(token)) {
            LOGGER.debug("setting access token for workspace manager request");
            apiClient.setAccessToken(token.toString());
        } else {
            LOGGER.warn("No access token found for workspace manager request.");
        }

        // return the client
        return apiClient;
    }

    public ReferencedGcpResourceApi getReferencedGcpResourceApi() {
        return new ReferencedGcpResourceApi(getApiClient());
    }

    public ResourceApi getResourceApi() {
        return new ResourceApi(getApiClient());
    }

    public ResourceApi getResourceApi(String token) {
        var apiClient = getApiClient();
        apiClient.setAccessToken(token);
        return new ResourceApi(apiClient);
    }

    public ControlledAzureResourceApi getAzureResourceApi() {
        return new ControlledAzureResourceApi(getApiClient());
    }

    public ControlledAzureResourceApi getAzureResourceApi(String token) {
        var apiClient = getApiClient();
        apiClient.setAccessToken(token);
        return new ControlledAzureResourceApi(apiClient);
    }
}