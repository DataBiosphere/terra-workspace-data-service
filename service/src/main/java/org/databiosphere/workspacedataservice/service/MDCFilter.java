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
import lombok.extern.slf4j.Slf4j;
import org.slf4j.MDC;
import org.springframework.stereotype.Component;

@Slf4j
@Component
public class MDCFilter implements Filter {

    static final String MDC_KEY = "requestId";
    public static final String RESPONSE_HEADER = "x-b3-traceid";

    // add other headers here, such as: x-vcap-request-id, X─B3─ParentSpanId, X─B3─SpanId, X─B3─Sampled
    static final List<String> INCOMING_HEADERS = Arrays.asList("x-b3-traceid", "x-request-id", "trace-id", "x-amzn-trace-id");


    // TODO: I think this has a problem with streaming responses
    @Override
    public void doFilter(ServletRequest servletRequest, ServletResponse servletResponse, FilterChain filterChain) throws IOException, ServletException {
        String uniqueId = null;
        // look for a unique requestid in the request
        if (servletRequest instanceof HttpServletRequest httpServletRequest) {
            for (String hdr : INCOMING_HEADERS) {
                String foundIncoming = httpServletRequest.getHeader(hdr);
                if (foundIncoming != null && !foundIncoming.isBlank()) {
                    uniqueId = foundIncoming;
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

        log.trace("Request IP address is {}", servletRequest.getRemoteAddr());
        log.trace("Request content type is {}", servletRequest.getContentType());

        if (servletResponse instanceof HttpServletResponse httpServletResponse) {
            httpServletResponse.setHeader(RESPONSE_HEADER, uniqueId);
            log.trace("Response header is set with uuid {}", uniqueId);
        }

        filterChain.doFilter(servletRequest, servletResponse);
    }
}
