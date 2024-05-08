package org.databiosphere.workspacedataservice.service;

import jakarta.servlet.*;
import jakarta.servlet.http.HttpServletResponse;
import java.io.IOException;

public class CacheControlNoStoreFilter implements Filter {
  @Override
  public void doFilter(ServletRequest request, ServletResponse response, FilterChain chain)
      throws IOException, ServletException {
    var servletResponse = (HttpServletResponse) response;
    servletResponse.setHeader("Cache-Control", "no-store");
    chain.doFilter(request, response);
  }
}
