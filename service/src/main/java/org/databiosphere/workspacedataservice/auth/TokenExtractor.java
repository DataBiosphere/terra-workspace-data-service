package org.databiosphere.workspacedataservice.auth;

import static org.databiosphere.workspacedataservice.sam.BearerTokenFilter.BEARER_PREFIX;

import java.util.Objects;
import javax.servlet.ServletRequest;
import javax.servlet.http.HttpServletRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpHeaders;

public class TokenExtractor {

  private static final Logger LOGGER = LoggerFactory.getLogger(TokenExtractor.class);

  private TokenExtractor() {}

  public static String getToken(ServletRequest request) {
    if (request instanceof HttpServletRequest httpRequest) {
      String authString = httpRequest.getHeader(HttpHeaders.AUTHORIZATION);
      if (!Objects.isNull(authString) && authString.startsWith(BEARER_PREFIX)) {
        String token = authString.replaceFirst(BEARER_PREFIX, "");
        LOGGER.debug("found bearer token in incoming request");
        return token;
      } else {
        LOGGER.debug("No bearer token in incoming request");
      }
    }
    return null;
  }
}
