package org.databiosphere.workspacedataservice.controller;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;
import org.springframework.web.servlet.config.annotation.AsyncSupportConfigurer;
import org.springframework.web.servlet.config.annotation.CorsRegistry;
import org.springframework.web.servlet.config.annotation.WebMvcConfigurer;

@Configuration
public class ControllerConfig {
  /**
   * Configure CORS response headers.
   *
   * <p>When running behind Terra's Azure Relay, the Relay handles CORS response headers, so WDS
   * should not; the two CORS configurations will conflict.
   *
   * <p>When running WDS locally for development - i.e. not behind a Relay - you may need to enable
   * headers. To do so, activate the "local" Spring profile by setting spring.profiles.active=local
   * in application.properties (or other Spring techniques for activating a profile)
   */
  @SuppressWarnings("java:S5122") // we explicitly want to allow * for origins
  @Bean
  @Profile("local-cors")
  public WebMvcConfigurer corsConfigurer() {
    return new WebMvcConfigurer() {
      @Override
      public void addCorsMappings(CorsRegistry registry) {
        registry
            .addMapping("/**")
            .allowedMethods("DELETE", "GET", "HEAD", "PATCH", "POST", "PUT")
            .allowedOrigins("*");
      }
    };
  }

  /** Configure the app for asynchronous request processing. */
  @Bean
  public WebMvcConfigurer asyncConfigurer() {
    return new WebMvcConfigurer() {
      @Override
      public void configureAsyncSupport(AsyncSupportConfigurer configurer) {
        configurer.setDefaultTimeout(-1);
      }
    };
  }
}
