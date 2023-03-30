package org.databiosphere.workspacedataservice.sam;

import org.apache.commons.lang3.StringUtils;
import org.broadinstitute.dsde.workbench.client.sam.ApiClient;
import org.broadinstitute.dsde.workbench.client.sam.api.ResourcesApi;
import org.broadinstitute.dsde.workbench.client.sam.api.StatusApi;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.web.context.request.RequestContextHolder;

import java.util.Objects;

import static org.databiosphere.workspacedataservice.sam.BearerTokenFilter.ATTRIBUTE_NAME_TOKEN;
import static org.springframework.web.context.request.RequestAttributes.SCOPE_REQUEST;

/**
 * Implementation of SamClientFactory that creates a Sam ApiClient, initializes that client with
 * the url to Sam, adds the current user's access token to the client, and then returns the
 * ResourcesApi from that client. ResourcesApi is the part of the Sam client used by WDS.
 */
public class HttpSamClientFactory implements SamClientFactory {

    private final String samUrl;

    private static final Logger LOGGER = LoggerFactory.getLogger(HttpSamClientFactory.class);


    public HttpSamClientFactory(String samUrl) {
        this.samUrl = samUrl;
    }

    private ApiClient getApiClient(String accessToken) {
        // create a new Sam client
        ApiClient apiClient = new ApiClient();
        // initialize the client with the url to Sam
        if (StringUtils.isNotBlank(samUrl)) {
            apiClient.setBasePath(samUrl);
        }
        if (accessToken == null){
            // grab the current user's bearer token (see BearerTokenFilter)
            if (RequestContextHolder.getRequestAttributes() != null) {
                Object token = RequestContextHolder.currentRequestAttributes()
                        .getAttribute(ATTRIBUTE_NAME_TOKEN, SCOPE_REQUEST);
                // add the user's bearer token to the client
                if (!Objects.isNull(token)) {
                    LOGGER.debug("setting access token for Sam request");
                    apiClient.setAccessToken(token.toString());
                } else {
                    LOGGER.warn("No access token found for Sam request.");
                }
            } else {
                LOGGER.warn("No explicit access token and no thread-bound token attribute found for Sam request.");
            }
        } else {
            apiClient.setAccessToken(accessToken);

        }
        // return the client
        return apiClient;
    }

    /**
     * Get a ResourcesApi Sam client, initialized with the url to Sam and the current user's
     * access token, if any
     * @return the usable Sam client
     */
    public ResourcesApi getResourcesApi(String token) {
        ApiClient apiClient = getApiClient(token);
        ResourcesApi resourcesApi = new ResourcesApi();
        resourcesApi.setApiClient(apiClient);
        return resourcesApi;
    }

    /**
     * Get a StatusApi Sam client, initialized with the url to Sam and the current user's
     * access token, if any
     * @return the usable Sam client
     */
    public StatusApi getStatusApi() {
        ApiClient apiClient = getApiClient(null);
        StatusApi statusApi = new StatusApi();
        statusApi.setApiClient(apiClient);
        return statusApi;
    }

}
