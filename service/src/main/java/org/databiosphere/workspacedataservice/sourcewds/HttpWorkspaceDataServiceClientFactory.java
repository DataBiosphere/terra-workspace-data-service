package org.databiosphere.workspacedataservice.sourcewds;

import org.databiosphere.workspacedata.api.CloningApi;
import org.databiosphere.workspacedata.client.ApiClient;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.web.context.request.RequestContextHolder;

import java.util.Objects;

import static org.databiosphere.workspacedataservice.sam.BearerTokenFilter.ATTRIBUTE_NAME_TOKEN;
import static org.springframework.web.context.request.RequestAttributes.SCOPE_REQUEST;

public class HttpWorkspaceDataServiceClientFactory implements WorkspaceDataServiceClientFactory {

    final String workspaceDataManagerUrl;
    private static final Logger LOGGER = LoggerFactory.getLogger(HttpWorkspaceDataServiceClientFactory.class);


    public HttpWorkspaceDataServiceClientFactory(String workspaceDataManagerUrl) {
        this.workspaceDataManagerUrl = workspaceDataManagerUrl;
    }

    private ApiClient getApiClient(String token) {
        // create a new data repo client
        ApiClient apiClient = new ApiClient();

        // initialize the client with the url to data repo
        if (StringUtils.isNotBlank(workspaceDataManagerUrl)) {
            apiClient.setBasePath(workspaceDataManagerUrl);
        }

        // grab the current user's bearer token (see BearerTokenFilter)
        if(token.isEmpty()) {
            Object userToken = RequestContextHolder.currentRequestAttributes()
                    .getAttribute(ATTRIBUTE_NAME_TOKEN, SCOPE_REQUEST);
            // add the user's bearer token to the client
            if (!Objects.isNull(token)) {
                LOGGER.debug("setting access token for workspace data service request");
                apiClient.setAccessToken(userToken.toString());
            } else {
                LOGGER.warn("No access token found for workspace data service request.");
            }
        }
        else {
            apiClient.setAccessToken(token);
        }

        // return the client
        return apiClient;
    }

    public CloningApi getBackupClient(String token) {
        return new CloningApi(getApiClient(token));
    }
}