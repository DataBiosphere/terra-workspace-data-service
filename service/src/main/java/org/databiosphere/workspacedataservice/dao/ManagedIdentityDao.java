package org.databiosphere.workspacedataservice.dao;

import com.azure.core.credential.AccessToken;
import com.azure.core.credential.TokenRequestContext;
import com.azure.identity.ManagedIdentityCredential;
import com.azure.identity.ManagedIdentityCredentialBuilder;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;

import java.util.Optional;

public class ManagedIdentityDao {

    @Value("${CLIENT_ID:}")
    private String clientId;

    private static final Logger LOGGER = LoggerFactory.getLogger(ManagedIdentityDao.class);

    /**
     * The default credential will use the user assigned managed identity with the specified client ID.
     */
    public String getAzureCredential() {
        if (StringUtils.isNotBlank(clientId)){
            ManagedIdentityCredential defaultCredential = new ManagedIdentityCredentialBuilder()
                    .clientId(clientId)
                    .build();

            TokenRequestContext requestContext = new TokenRequestContext().addScopes("https://management.azure.com/.default");
            Optional<AccessToken> optionalToken = Optional.ofNullable(defaultCredential.getToken(requestContext).block());
            return optionalToken.map(AccessToken::getToken).orElse(null);
        } else {
            LOGGER.warn("No clientId provided; wds-instance resource and default schema not created");
            return null;
        }
    }
}
