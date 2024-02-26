package org.databiosphere.workspacedataservice.controller;

import java.io.IOException;
import java.nio.file.Files;
import org.databiosphere.workspacedataservice.annotations.DeploymentMode.ControlPlane;
import org.databiosphere.workspacedataservice.annotations.DeploymentMode.DataPlane;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.Resource;

/**
 * Reads either swagger-ui-control-plane.html or swagger-ui-data-plane.html, depending on the
 * profile with which this deployment was started, and caches the contents of those HTML files into
 * a String bean.
 *
 * @see org.databiosphere.workspacedataservice.controller.SwaggerUiController
 */
@Configuration
public class SwaggerUiConfig {

  @ControlPlane
  @Bean(name = "swaggerHtml")
  public String controlPlaneSwaggerHtml(
      @Value("classpath:swagger-ui-control-plane.html") Resource swaggerUiControlPlaneResource)
      throws IOException {
    return new String(Files.readAllBytes(swaggerUiControlPlaneResource.getFile().toPath()));
  }

  @DataPlane
  @Bean(name = "swaggerHtml")
  public String dataPlaneSwaggerHtml(
      @Value("classpath:swagger-ui-data-plane.html") Resource swaggerUiDataPlaneResource)
      throws IOException {
    return new String(Files.readAllBytes(swaggerUiDataPlaneResource.getFile().toPath()));
  }
}
