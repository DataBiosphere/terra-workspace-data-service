package org.databiosphere.workspacedataservice.service;

import jakarta.servlet.FilterChain;
import jakarta.servlet.ServletException;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import java.io.IOException;
import org.springframework.stereotype.Component;
import org.springframework.web.filter.OncePerRequestFilter;

/**
 * API responses contain potentially sensitive data and thus should not be cached. This configures a
 * filter that adds "Cache-Control: no-store" and "Pragma: no-cache" headers to API responses.
 */
@Component
public class NoCacheFilter extends OncePerRequestFilter {
  @Override
  protected void doFilterInternal(
      HttpServletRequest request, HttpServletResponse response, FilterChain chain)
      throws IOException, ServletException {
    response.setHeader("Cache-Control", "no-store");
    response.setHeader("Pragma", "no-cache");
    chain.doFilter(request, response);
  }

  /** Allow Swagger UI static resources to be cached. */
  @Override
  protected boolean shouldNotFilter(HttpServletRequest request) {
    String path = request.getServletPath();
    return path.startsWith("/webjars/swagger-ui-dist/");
  }
}
