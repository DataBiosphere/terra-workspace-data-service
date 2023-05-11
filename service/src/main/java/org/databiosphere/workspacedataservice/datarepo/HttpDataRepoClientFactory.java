package org.databiosphere.workspacedataservice.datarepo;

import bio.terra.datarepo.api.RepositoryApi;
import org.apache.commons.lang3.StringUtils;
import bio.terra.datarepo.client.ApiClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.web.context.request.RequestContextHolder;

import java.util.Objects;

import static org.databiosphere.workspacedataservice.sam.BearerTokenFilter.ATTRIBUTE_NAME_TOKEN;
import static org.springframework.web.context.request.RequestAttributes.SCOPE_REQUEST;

public class HttpDataRepoClientFactory implements DataRepoClientFactory {

    String dataRepoUrl;
    private static final Logger LOGGER = LoggerFactory.getLogger(HttpDataRepoClientFactory.class);


    public HttpDataRepoClientFactory(String dataRepoUrl) {
        this.dataRepoUrl = dataRepoUrl;
    }

    private ApiClient getApiClient() {
        // create a new data repo client
        ApiClient apiClient = new ApiClient();

        // initialize the client with the url to data repo
        if (StringUtils.isNotBlank(dataRepoUrl)) {
            apiClient.setBasePath(dataRepoUrl);
        }
        // grab the current user's bearer token (see BearerTokenFilter)
        Object token = RequestContextHolder.currentRequestAttributes()
                .getAttribute(ATTRIBUTE_NAME_TOKEN, SCOPE_REQUEST);
        // add the user's bearer token to the client
        if (!Objects.isNull(token)) {
            LOGGER.debug("setting access token for data repo request");
            apiClient.setAccessToken(token.toString());
        } else {
            LOGGER.warn("No access token found for data repo request.");
        }

        // return the client
        return apiClient;
    }

    public RepositoryApi getRepositoryApi(){
        return new RepositoryApi(getApiClient());
    }


}
