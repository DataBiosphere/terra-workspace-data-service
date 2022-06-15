package org.databiosphere.workspacedataservice;

import org.databiosphere.workspacedataservice.shared.model.secrets.DummySecret;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.vault.authentication.ClientAuthentication;
import org.springframework.vault.authentication.TokenAuthentication;
import org.springframework.vault.client.VaultEndpoint;
import org.springframework.vault.config.AbstractVaultConfiguration;
import org.springframework.vault.core.VaultTemplate;
import org.springframework.vault.support.VaultResponseSupport;

import java.io.IOException;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;

@Configuration
public class AppConfig extends AbstractVaultConfiguration {

    @Value("${vault.uri}")
    URI vaultUri;

    @Value("${vault.token-path}")
    String vaultTokenPath;

    @Value("${vault.env}")
    String vaultEnv;

    private static final Logger LOGGER = LoggerFactory.getLogger(AppConfig.class);

    @Override
    public VaultEndpoint vaultEndpoint() {
        return VaultEndpoint.from(vaultUri);                          
    }

    @Override
    public ClientAuthentication clientAuthentication() {
        String token;
        try {
            token = Files.readString(Path.of(vaultTokenPath));
            LOGGER.info("Successfully retrieved vault token at path {}", vaultTokenPath);
        } catch (IOException e) {
            throw new RuntimeException(String.format("Vault token file not found: %s", vaultTokenPath));
        }

        return new TokenAuthentication(token);
    }

    @Bean
    public DummySecret dummySecret(VaultTemplate vaultTemplate) {
        LOGGER.info("Loading secret at path {}", wdsSecretsPath("dummy-secret"));
        VaultResponseSupport<DummySecret> response =
                vaultTemplate.read(wdsSecretsPath("dummy-secret"), DummySecret.class);
        assert response != null; // TODO log missing secret error
        return response.getData();
    }

    private String wdsSecretsPath(String relativePath) {
        return String.format("secret/dsde/%s/workspace-data-service/%s", vaultEnv, relativePath);
    }
}
