package org.databiosphere.workspacedataservice.sam;

import static org.springframework.web.context.request.RequestAttributes.SCOPE_REQUEST;

import java.io.IOException;
import javax.servlet.Filter;
import javax.servlet.FilterChain;
import javax.servlet.ServletException;
import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;
import org.databiosphere.workspacedataservice.auth.TokenExtractor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;
import org.springframework.web.context.request.RequestAttributes;
import org.springframework.web.context.request.RequestContextHolder;

/**
 * Servlet filter that inspects the incoming request for an Authorization: Bearer ... token, and
 * saves any token it finds into the current thread-local RequestContextHolder.
 *
 * <p>Note that this filter does not validate or inspect the token; it just extracts it from the
 * request, allowing it to be sent as-is from WDS to other services such as Sam.
 */
@Component
public class BearerTokenFilter implements Filter {
  private static final Logger LOGGER = LoggerFactory.getLogger(BearerTokenFilter.class);

  public static final String ATTRIBUTE_NAME_TOKEN = "bearer-token-attribute";

  public static final String BEARER_PREFIX = "Bearer ";

  @Override
  public void doFilter(ServletRequest request, ServletResponse response, FilterChain chain)
      throws IOException, ServletException {

    String token = TokenExtractor.getToken(request);
    if (token != null) {
      RequestAttributes currentAttributes = RequestContextHolder.currentRequestAttributes();
      currentAttributes.setAttribute(ATTRIBUTE_NAME_TOKEN, token, SCOPE_REQUEST);
      RequestContextHolder.setRequestAttributes(currentAttributes);
    }

    chain.doFilter(request, response);
  }
}
