package org.databiosphere.workspacedataservice.annotations;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;

/** Conditional annotations for deployment mode-specific beans. */
public interface DeploymentMode {

  /**
   * The annotated type or method is conditionally enabled when the deployment mode is
   * control-plane.
   */
  @Target({ElementType.TYPE, ElementType.METHOD})
  @Retention(RetentionPolicy.RUNTIME)
  @ConditionalOnProperty(value = "env.wds.deploymentMode", havingValue = "control-plane")
  @interface ControlPlane {}

  /**
   * The annotated type or method is conditionally enabled when the deployment mode is data-plane.
   */
  @Target({ElementType.TYPE, ElementType.METHOD})
  @Retention(RetentionPolicy.RUNTIME)
  @ConditionalOnProperty(value = "env.wds.deploymentMode", havingValue = "data-plane")
  @interface DataPlane {}
}
