package org.databiosphere.workspacedataservice.sam;

import static org.springframework.web.context.request.RequestAttributes.SCOPE_REQUEST;

import jakarta.servlet.Filter;
import jakarta.servlet.FilterChain;
import jakarta.servlet.ServletException;
import jakarta.servlet.ServletRequest;
import jakarta.servlet.ServletResponse;
import jakarta.servlet.http.HttpServletRequest;
import java.io.IOException;
import java.util.Objects;
import org.databiosphere.workspacedataservice.shared.model.BearerToken;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpHeaders;
import org.springframework.stereotype.Component;
import org.springframework.web.context.request.RequestAttributes;
import org.springframework.web.context.request.RequestContextHolder;

/**
 * Servlet filter that inspects the incoming request for an Authorization: Bearer ... token, and
 * saves any token it finds into the current thread-local RequestContextHolder and the
 * BearerTokenHolder.
 *
 * <p>Note that this filter does not validate or inspect the token; it just extracts it from the
 * request, allowing it to be sent as-is from WDS to other services such as Sam.
 */
@Component
public class BearerTokenFilter implements Filter {
  private static final Logger LOGGER = LoggerFactory.getLogger(BearerTokenFilter.class);

  public static final String ATTRIBUTE_NAME_TOKEN = "bearer-token-attribute";

  private static final String BEARER_PREFIX = "Bearer ";
  private final BearerTokenHolder bearerTokenHolder;

  BearerTokenFilter(BearerTokenHolder bearerTokenHolder) {
    this.bearerTokenHolder = bearerTokenHolder;
  }

  @Override
  public void doFilter(ServletRequest request, ServletResponse response, FilterChain chain)
      throws IOException, ServletException {
    if (request instanceof HttpServletRequest httpRequest) {
      String authString = httpRequest.getHeader(HttpHeaders.AUTHORIZATION);
      if (!Objects.isNull(authString) && authString.startsWith(BEARER_PREFIX)) {
        String token = authString.replaceFirst(BEARER_PREFIX, "");
        LOGGER.debug("found bearer token in incoming request");
        saveTokenToRequestContext(token);
        bearerTokenHolder.setToken(BearerToken.of(token));
      } else {
        LOGGER.debug("No bearer token in incoming request");
      }
    }

    chain.doFilter(request, response);
  }

  private void saveTokenToRequestContext(String token) {
    RequestAttributes currentAttributes = RequestContextHolder.currentRequestAttributes();
    currentAttributes.setAttribute(ATTRIBUTE_NAME_TOKEN, token, SCOPE_REQUEST);
    RequestContextHolder.setRequestAttributes(currentAttributes);
  }
}
