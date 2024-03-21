package org.databiosphere.workspacedataservice.rawls;

import java.util.Objects;
import org.databiosphere.workspacedataservice.sam.TokenContextUtil;
import org.databiosphere.workspacedataservice.shared.model.BearerToken;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.client.ClientHttpRequest;
import org.springframework.http.client.ClientHttpRequestInitializer;

/**
 * RequestInitializer for use in RestClient implementations: look for a bearer auth token via
 * TokenContextUtil, and add that token - if found - to the outbound RestClient headers.
 */
public class BearerAuthRequestInitializer implements ClientHttpRequestInitializer {

  private static final Logger LOGGER = LoggerFactory.getLogger(BearerAuthRequestInitializer.class);

  @Override
  public void initialize(@NotNull ClientHttpRequest request) {
    BearerToken token = TokenContextUtil.getToken();

    if (token.nonEmpty()) {
      LOGGER.debug("setting access token for request");
      request.getHeaders().setBearerAuth(Objects.requireNonNull(token.getValue()));
    } else {
      LOGGER.warn("No access token found for request.");
    }
  }
}
