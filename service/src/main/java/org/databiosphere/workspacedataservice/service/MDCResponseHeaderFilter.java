package org.databiosphere.workspacedataservice.service;

import org.slf4j.MDC;
import org.springframework.stereotype.Component;

import jakarta.servlet.*;
import jakarta.servlet.http.HttpServletResponse;
import java.io.IOException;

/**
 * Servlet filter that inspects the SLF4J MDC context and propagates any "requestId" value it finds in MDC
 * to an "x-b3-traceid" response header.
 * <p>
 * See also {@link MDCServletRequestListener}, which looks in the MDC context for a request id and adds
 * it to response headers if found.
 */
@Component
public class MDCResponseHeaderFilter implements Filter {

    @Override
    public void doFilter(ServletRequest request, ServletResponse response, FilterChain chain) throws IOException, ServletException {
        String uniqueId = MDC.get(MDCServletRequestListener.MDC_KEY);
        if (uniqueId != null && response instanceof HttpServletResponse httpServletResponse) {
            httpServletResponse.setHeader(MDCServletRequestListener.RESPONSE_HEADER, uniqueId);
        }
        chain.doFilter(request, response);
    }

}
