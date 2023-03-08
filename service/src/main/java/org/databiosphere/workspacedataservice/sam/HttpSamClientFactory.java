package org.databiosphere.workspacedataservice.sam;

import org.apache.commons.lang3.StringUtils;
import org.broadinstitute.dsde.workbench.client.sam.ApiClient;
import org.broadinstitute.dsde.workbench.client.sam.api.ResourcesApi;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.web.context.request.RequestContextHolder;

import java.util.Objects;

import static org.databiosphere.workspacedataservice.sam.BearerTokenFilter.ATTRIBUTE_NAME_TOKEN;
import static org.springframework.web.context.request.RequestAttributes.SCOPE_REQUEST;

public class HttpSamClientFactory implements SamClientFactory {

    private final String samUrl;

    private static final Logger LOGGER = LoggerFactory.getLogger(HttpSamClientFactory.class);


    public HttpSamClientFactory(String samUrl) {
        this.samUrl = samUrl;
    }

    private ApiClient getApiClient() {
        ApiClient apiClient = new ApiClient();
        if (StringUtils.isNotBlank(samUrl)) {
            apiClient.setBasePath(samUrl);
        }

        Object token = RequestContextHolder.currentRequestAttributes()
                .getAttribute(ATTRIBUTE_NAME_TOKEN, SCOPE_REQUEST);

        if (!Objects.isNull(token)) {
            LOGGER.debug("setting access token for Sam request: {}", BearerTokenFilter.loggableToken(token.toString()));
            apiClient.setAccessToken(token.toString());
        } else {
            LOGGER.warn("No access token found for Sam request.");
        }

        return apiClient;
    }


    public ResourcesApi getResourcesApi() {
        ApiClient apiClient = getApiClient();
        ResourcesApi resourcesApi = new ResourcesApi();
        resourcesApi.setApiClient(apiClient);
        return resourcesApi;
    }


}
