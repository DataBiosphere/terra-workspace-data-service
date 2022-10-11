package org.databiosphere.workspacedataservice.service;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;
import javax.servlet.Filter;
import javax.servlet.FilterChain;
import javax.servlet.ServletException;
import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;
import org.springframework.stereotype.Component;

@Component
public class MDCFilter implements Filter {

    private static final Logger LOGGER = LoggerFactory.getLogger(MDCFilter.class);

    // where the unique request id is stored in the MDC context
    static final String MDC_KEY = "requestId";
    // the response header containing the unique request id
    public static final String RESPONSE_HEADER = "x-b3-traceid";
    // the list of request headers, in order, we will search for an incoming unique request id
    // we could add other headers here, such as: x-vcap-request-id, X─B3─ParentSpanId, X─B3─SpanId, X─B3─Sampled, x-amzn-trace-id
    static final List<String> INCOMING_HEADERS = Arrays.asList("x-b3-traceid", "x-request-id", "trace-id");

    @Override
    public void doFilter(ServletRequest servletRequest, ServletResponse servletResponse, FilterChain filterChain) throws IOException, ServletException {
        String uniqueId = null;
        // look for a non-blank unique request id in the incoming request headers
        if (servletRequest instanceof HttpServletRequest httpServletRequest) {
            for (String hdr : INCOMING_HEADERS) {
                String foundIncoming = httpServletRequest.getHeader(hdr);
                if (foundIncoming != null && !foundIncoming.isBlank()) {
                    uniqueId = foundIncoming.length() > 64 ? foundIncoming.substring(0,64) : foundIncoming;
                    break;
                }
            }
        }

        // if we did not find an id in incoming headers, make a unique one
        if (uniqueId == null) {
            uniqueId = UUID.randomUUID().toString();
        }

        // add id to MDC logging
        MDC.put(MDC_KEY, uniqueId);

        LOGGER.trace("Request IP address is {}", servletRequest.getRemoteAddr());
        LOGGER.trace("Request content type is {}", servletRequest.getContentType());

        // add id to the response
        if (servletResponse instanceof HttpServletResponse httpServletResponse) {
            httpServletResponse.setHeader(RESPONSE_HEADER, uniqueId);
            LOGGER.trace("Response header is set with uuid {}", uniqueId);
        }

        filterChain.doFilter(servletRequest, servletResponse);
    }
}
