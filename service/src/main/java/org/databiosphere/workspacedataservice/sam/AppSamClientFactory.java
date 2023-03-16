package org.databiosphere.workspacedataservice.sam;

import com.azure.core.credential.AccessToken;
import com.azure.core.credential.TokenRequestContext;
import com.azure.identity.DefaultAzureCredential;
import com.azure.identity.DefaultAzureCredentialBuilder;
import com.azure.identity.ManagedIdentityCredential;
import com.azure.identity.ManagedIdentityCredentialBuilder;
import org.apache.commons.lang3.StringUtils;
import org.broadinstitute.dsde.workbench.client.sam.ApiClient;
import org.broadinstitute.dsde.workbench.client.sam.api.ResourcesApi;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import reactor.core.publisher.Mono;

import java.util.Objects;

public class AppSamClientFactory implements SamClientFactory {

    private final String samUrl;
    //TODO what to do for local development
    @Value("${CLIENT_ID:}")
    private String clientId;

    private static final Logger LOGGER = LoggerFactory.getLogger(HttpSamClientFactory.class);


    public AppSamClientFactory(String samUrl) {
        this.samUrl = samUrl;
    }

    private ApiClient getApiClient() {
        // create a new Sam client
        ApiClient apiClient = new ApiClient();
        // initialize the client with the url to Sam
        if (StringUtils.isNotBlank(samUrl)) {
            apiClient.setBasePath(samUrl);
        }
        // get an azure managed identy token
        String token = getAzureCredential(clientId);

        // add the user's bearer token to the client
        if (!Objects.isNull(token)) {
            LOGGER.debug("setting access token for Sam request");
            apiClient.setAccessToken(token);
        } else {
            LOGGER.warn("No access token found for Sam request.");
        }
        // return the client
        return apiClient;
    }

    /**
     * Get a ResourcesApi Sam client, initialized with the url to Sam and the current user's
     * access token, if any
     * @return the usable Sam client
     */
    public ResourcesApi getResourcesApi() {
        ApiClient apiClient = getApiClient();
        ResourcesApi resourcesApi = new ResourcesApi();
        resourcesApi.setApiClient(apiClient);
        return resourcesApi;
    }

    /**
     * The default credential will use the user assigned managed identity with the specified client ID.
     */
    public String getAzureCredential(String clientId) {
        ManagedIdentityCredential defaultCredential = new ManagedIdentityCredentialBuilder()
                .clientId(clientId)
                .build();

        TokenRequestContext requestContext = new TokenRequestContext().addScopes("https://management.azure.com/.default");
        Mono<AccessToken> token = defaultCredential.getToken(requestContext);
        return token.block().getToken();
    }
}
