package org.databiosphere.workspacedataservice.controller;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import org.databiosphere.workspacedataservice.annotations.DeploymentMode.ControlPlane;
import org.databiosphere.workspacedataservice.annotations.DeploymentMode.DataPlane;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.Resource;
import org.springframework.util.StreamUtils;

/**
 * Reads either swagger-ui-control-plane[-preview].html or swagger-ui-control-plane.html or
 * swagger-ui-data-plane.html, depending on the profile and preview flag with which this deployment
 * was started, and caches the contents of those HTML files into a String bean.
 *
 * @see org.databiosphere.workspacedataservice.controller.SwaggerUiController
 */
@Configuration
public class SwaggerUiConfig {

  @ControlPlane
  @Bean(name = "swaggerHtml")
  public String controlPlaneSwaggerHtml(
      @Value("${controlPlanePreview:off}") String controlPlanePreview,
      @Value("classpath:swagger-ui-control-plane-preview.html")
          Resource swaggerUiControlPlanePreviewResource,
      @Value("classpath:swagger-ui-control-plane.html") Resource swaggerUiControlPlaneResource)
      throws IOException {
    Resource resourceToLoad =
        "on".equals(controlPlanePreview)
            ? swaggerUiControlPlanePreviewResource
            : swaggerUiControlPlaneResource;
    return StreamUtils.copyToString(resourceToLoad.getInputStream(), StandardCharsets.UTF_8);
  }

  @DataPlane
  @Bean(name = "swaggerHtml")
  public String dataPlaneSwaggerHtml(
      @Value("classpath:swagger-ui-data-plane.html") Resource swaggerUiDataPlaneResource)
      throws IOException {
    return StreamUtils.copyToString(
        swaggerUiDataPlaneResource.getInputStream(), StandardCharsets.UTF_8);
  }
}
