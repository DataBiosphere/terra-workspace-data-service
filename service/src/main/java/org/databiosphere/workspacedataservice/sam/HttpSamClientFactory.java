package org.databiosphere.workspacedataservice.sam;

import okhttp3.OkHttpClient;
import org.apache.commons.lang3.StringUtils;
import org.broadinstitute.dsde.workbench.client.sam.ApiClient;
import org.broadinstitute.dsde.workbench.client.sam.api.ResourcesApi;
import org.broadinstitute.dsde.workbench.client.sam.api.StatusApi;
import org.broadinstitute.dsde.workbench.client.sam.api.UsersApi;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.web.context.request.RequestContextHolder;

import java.util.Objects;
import java.util.Optional;

import static org.databiosphere.workspacedataservice.sam.BearerTokenFilter.ATTRIBUTE_NAME_TOKEN;
import static org.springframework.web.context.request.RequestAttributes.SCOPE_REQUEST;

/**
 * Implementation of SamClientFactory that creates a Sam ApiClient, initializes that client with
 * the url to Sam, adds the current user's access token to the client, and then returns the
 * ResourcesApi from that client. ResourcesApi is the part of the Sam client used by WDS.
 */
public class HttpSamClientFactory implements SamClientFactory {

    public static final String USE_BEARER_TOKEN_IF_PRESENT = null;

    private final String samUrl;
    private final OkHttpClient commonHttpClient;

    private static final Logger LOGGER = LoggerFactory.getLogger(HttpSamClientFactory.class);

    public HttpSamClientFactory(String samUrl) {
        this.samUrl = samUrl;
        this.commonHttpClient = new ApiClient()
                .getHttpClient()
                .newBuilder()
                .build();
        // TODO: add tracing interceptor for distributed tracing to Sam.
        // this requires we import terra-common-lib
    }

    private ApiClient getApiClient(String accessToken) {
        // create a new Sam client
        ApiClient apiClient = new ApiClient();
        apiClient.setHttpClient(commonHttpClient);
        // initialize the client with the url to Sam
        if (StringUtils.isNotBlank(samUrl)) {
            apiClient.setBasePath(samUrl);
        }

        // set the provided access token if present, otherwise fall
        // back on the token in the current request context
        Optional.ofNullable(accessToken)
                .or(HttpSamClientFactory::getCurrentUserBearerToken)
                .ifPresent(apiClient::setAccessToken);

        // return the client
        return apiClient;
    }

    private static Optional<String> getCurrentUserBearerToken() {
        if (Objects.isNull(RequestContextHolder.getRequestAttributes())) {
            LOGGER.warn("No request context available to check access token for Sam request.");
            return Optional.empty();
        }

        // grab the current user's bearer token (see BearerTokenFilter)
        Object token = RequestContextHolder.currentRequestAttributes()
                .getAttribute(ATTRIBUTE_NAME_TOKEN, SCOPE_REQUEST);

        if (Objects.isNull(token)) {
            LOGGER.warn("No access token found for Sam request.");
            return Optional.empty();
        }

        // add the user's bearer token to the client
        return Optional.of(token.toString());
    }

    /**
     * Get a ResourcesApi Sam client, initialized with the url to Sam and the current user's
     * access token, if any
     *
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
     *
     * @return the usable Sam client
     */
    public StatusApi getStatusApi() {
        ApiClient apiClient = getApiClient(USE_BEARER_TOKEN_IF_PRESENT);
        StatusApi statusApi = new StatusApi();
        statusApi.setApiClient(apiClient);
        return statusApi;
    }

    public UsersApi getUsersApi(String token) {
        ApiClient apiClient = getApiClient(token);
        UsersApi usersApi = new UsersApi();
        usersApi.setApiClient(apiClient);
        return usersApi;
    }
}
