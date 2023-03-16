package org.databiosphere.workspacedataservice.sam;

import com.azure.core.credential.AccessToken;
import com.azure.core.credential.TokenRequestContext;
import com.azure.identity.DefaultAzureCredential;
import com.azure.identity.DefaultAzureCredentialBuilder;
import org.apache.commons.lang3.StringUtils;
import org.broadinstitute.dsde.workbench.client.sam.ApiClient;
import org.broadinstitute.dsde.workbench.client.sam.api.ResourcesApi;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Mono;

import java.util.Objects;

public class AppSamClientFactory implements SamClientFactory {

    private final String samUrl;
    //TODO where is this coming from
    private final String clientId = "managed_identity_id";

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
        Mono<AccessToken> token = getAzureCredential(clientId);
        LOGGER.debug("token: " + token.toString());
        // add the user's bearer token to the client
        if (!Objects.isNull(token)) {
            LOGGER.debug("setting access token for Sam request");
            apiClient.setAccessToken(token.toString());
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
    public Mono<AccessToken> getAzureCredential(String clientId) {
        DefaultAzureCredential defaultCredential = new DefaultAzureCredentialBuilder()
                .managedIdentityClientId(clientId)
                .build();

        TokenRequestContext requestContext = new TokenRequestContext();
        Mono<AccessToken> token = defaultCredential.getToken(requestContext);
        return token;

        // Azure SDK client builders accept the credential as a parameter
//        SecretClient client = new SecretClientBuilder()
//                .vaultUrl("https://{YOUR_VAULT_NAME}.vault.azure.net")
//                .credential(defaultCredential)
//                .buildClient();
    }
}
