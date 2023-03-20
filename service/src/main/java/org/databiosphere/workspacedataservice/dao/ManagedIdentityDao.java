package org.databiosphere.workspacedataservice.dao;

import com.azure.core.credential.AccessToken;
import com.azure.core.credential.TokenRequestContext;
import com.azure.identity.ManagedIdentityCredential;
import com.azure.identity.ManagedIdentityCredentialBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Repository;
import reactor.core.publisher.Mono;

@Repository
public class ManagedIdentityDao {

    //TODO what to do for local development
    @Value("${CLIENT_ID:}")
    private String clientId;

    private static final Logger LOGGER = LoggerFactory.getLogger(ManagedIdentityDao.class);

    /**
     * The default credential will use the user assigned managed identity with the specified client ID.
     */
    public String getAzureCredential() {
        ManagedIdentityCredential defaultCredential = new ManagedIdentityCredentialBuilder()
                .clientId(clientId)
                .build();

        TokenRequestContext requestContext = new TokenRequestContext().addScopes("https://management.azure.com/.default");
        Mono<AccessToken> token = defaultCredential.getToken(requestContext);
        try {
            //TODO should token.block() be assigned to a variable?  can I call it twice?
            return token.block().getToken();
        } catch (NullPointerException e){
            LOGGER.warn("No token acquired from azure managed identity");
            return null;
        }
    }
}
