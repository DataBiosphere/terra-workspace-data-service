package org.databiosphere.workspacedataservice.annotations;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import org.springframework.context.annotation.Profile;

/** Conditional annotations for deployment mode-specific beans. */
public interface DeploymentMode {

  /**
   * The annotated type or method is conditionally enabled when the deployment mode is
   * control-plane. As of this writing, this equates to cWDS in GCP.
   */
  @Target({ElementType.TYPE, ElementType.METHOD})
  @Retention(RetentionPolicy.RUNTIME)
  @Profile("control-plane")
  @interface ControlPlane {}

  /**
   * The annotated type or method is conditionally enabled when the deployment mode is data-plane.
   * As of this writing, this equates to a WDS associated with a workspace running as an Azure app
   * in a customer Landing Zone.
   */
  @Target({ElementType.TYPE, ElementType.METHOD})
  @Retention(RetentionPolicy.RUNTIME)
  @Profile("data-plane")
  @interface DataPlane {}
}
