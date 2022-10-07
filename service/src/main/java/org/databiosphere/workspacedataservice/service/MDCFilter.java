package org.databiosphere.workspacedataservice.service;

import java.io.IOException;
import java.util.UUID;
import javax.servlet.Filter;
import javax.servlet.FilterChain;
import javax.servlet.ServletException;
import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;
import javax.servlet.http.HttpServletResponse;
import lombok.extern.slf4j.Slf4j;
import org.slf4j.MDC;
import org.springframework.stereotype.Component;
import org.springframework.web.util.ContentCachingResponseWrapper;

@Slf4j
@Component
public class MDCFilter implements Filter {

    static final String MDC_KEY = "requestId";
    static final String RESPONSE_HEADER = "requestId";

    // TODO: I think this has a problem with streaming responses
    @Override
    public void doFilter(ServletRequest servletRequest, ServletResponse servletResponse, FilterChain filterChain) throws IOException, ServletException {
        UUID uniqueId = UUID.randomUUID();
        MDC.put(MDC_KEY, uniqueId.toString());
        log.debug("Request IP address is {}", servletRequest.getRemoteAddr());
        log.debug("Request content type is {}", servletRequest.getContentType());
        HttpServletResponse httpServletResponse = (HttpServletResponse) servletResponse;
        ContentCachingResponseWrapper responseWrapper = new ContentCachingResponseWrapper(
                httpServletResponse
        );
        filterChain.doFilter(servletRequest, responseWrapper);
        responseWrapper.setHeader(RESPONSE_HEADER, uniqueId.toString());
        responseWrapper.copyBodyToResponse();
        log.debug("Response header is set with uuid {}", responseWrapper.getHeader(RESPONSE_HEADER));
    }
}
