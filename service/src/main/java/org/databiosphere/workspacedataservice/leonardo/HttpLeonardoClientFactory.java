package org.databiosphere.workspacedataservice.leonardo;

import okhttp3.OkHttpClient;
import org.apache.commons.lang3.StringUtils;
import org.broadinstitute.dsde.workbench.client.leonardo.ApiClient;
import org.broadinstitute.dsde.workbench.client.leonardo.api.AppsV2Api;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.web.context.request.RequestContextHolder;

import java.util.Objects;

import static org.databiosphere.workspacedataservice.sam.BearerTokenFilter.ATTRIBUTE_NAME_TOKEN;
import static org.springframework.web.context.request.RequestAttributes.SCOPE_REQUEST;

public class HttpLeonardoClientFactory implements LeonardoClientFactory {

    final String leoEndpointUrl;
    private final OkHttpClient commonHttpClient;
    private static final Logger LOGGER = LoggerFactory.getLogger(HttpLeonardoClientFactory.class);


    public HttpLeonardoClientFactory(String leoUrl) {
        this.leoEndpointUrl = leoUrl;
        this.commonHttpClient = new ApiClient()
                .getHttpClient()
                .newBuilder()
                .build();
    }

    private ApiClient getApiClient(String token) {
        // create a new data repo client
        ApiClient apiClient = new ApiClient();
        apiClient.setHttpClient(commonHttpClient);

        // initialize the client with the url to leo endpoint
        if (StringUtils.isNotBlank(leoEndpointUrl)) {
            apiClient.setBasePath(leoEndpointUrl);
        }

        // grab the current user's bearer token (see BearerTokenFilter)
        if(token.isEmpty()) {
            Object userToken = RequestContextHolder.currentRequestAttributes()
                    .getAttribute(ATTRIBUTE_NAME_TOKEN, SCOPE_REQUEST);
            // add the user's bearer token to the client
            if (!Objects.isNull(userToken)) {
                LOGGER.debug("setting access token for leonardo request");
                apiClient.setAccessToken(userToken.toString());
            } else {
                LOGGER.warn("No access token found for leonardo request.");
            }
        }
        else {
            apiClient.setAccessToken(token);
        }

        // return the client
        return apiClient;
    }

    public AppsV2Api getAppsV2Api(String token) {
        return new AppsV2Api(getApiClient(token));
    }
}