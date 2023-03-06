package org.databiosphere.workspacedataservice.sam;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;
import org.springframework.web.context.request.RequestAttributes;
import org.springframework.web.context.request.RequestContextHolder;

import javax.servlet.Filter;
import javax.servlet.FilterChain;
import javax.servlet.ServletException;
import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;
import javax.servlet.http.HttpServletRequest;

import java.io.IOException;
import java.util.Objects;

import static org.springframework.web.context.request.RequestAttributes.SCOPE_REQUEST;

/**
 * Servlet filter that inspects the incoming request for an Authorization: Bearer ... token,
 * and saves any token it finds into the current thread-local RequestContextHolder.
 * <p>
 * Note that this filter does not validate or inspect the token; it just extracts it from the
 * request, allowing it to be sent as-is from WDS to other services such as Sam.
 */
@Component
public class BearerTokenFilter implements Filter {
    private static final Logger LOGGER = LoggerFactory.getLogger(BearerTokenFilter.class);

    public static final String ATTRIBUTE_NAME_TOKEN = "bearer-token-attribute";

    // TODO: unit tests

    @Override
    public void doFilter(ServletRequest request, ServletResponse response, FilterChain chain) throws IOException, ServletException {
        if (request instanceof HttpServletRequest httpRequest) {
            // TODO: more robust token extraction?
            String authString = httpRequest.getHeader("Authorization");
            if (!Objects.isNull(authString)) {
                String token = authString.replaceFirst("Bearer ", "");

                LOGGER.info("found bearer token in incoming request: {}", loggableToken(token));

                RequestAttributes currentAttributes = RequestContextHolder.currentRequestAttributes();
                currentAttributes.setAttribute(ATTRIBUTE_NAME_TOKEN, token, SCOPE_REQUEST);
                RequestContextHolder.setRequestAttributes(currentAttributes);
            } else {
                LOGGER.info("No bearer token in incoming request");
            }
        }

        chain.doFilter(request, response);
    }

    /**
     * Utility method which grabs the last 6 chars of the auth token to allow it to
     * be logged without causing a security problem.
     * @param token full token string from the incoming request
     * @return loggable substring of the original token
     */
    public static String loggableToken(String token) {
        int end = token.length();
        int start = end - 6;
        if (start < 0) {
            start = 0;
        }
        return  "..." + token.substring(start, end);
    }


}
