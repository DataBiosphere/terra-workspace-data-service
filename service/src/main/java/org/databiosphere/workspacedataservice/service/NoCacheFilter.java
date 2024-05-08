package org.databiosphere.workspacedataservice.service;

import jakarta.servlet.*;
import jakarta.servlet.http.HttpServletResponse;
import java.io.IOException;

public class NoCacheFilter implements Filter {
  @Override
  public void doFilter(ServletRequest request, ServletResponse response, FilterChain chain)
      throws IOException, ServletException {
    if (response instanceof HttpServletResponse httpResponse) {
      httpResponse.setHeader("Cache-Control", "no-store");
      httpResponse.setHeader("Pragma", "no-cache");
    }
    chain.doFilter(request, response);
  }
}
