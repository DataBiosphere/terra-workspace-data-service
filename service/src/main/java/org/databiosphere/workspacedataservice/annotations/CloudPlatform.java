package org.databiosphere.workspacedataservice.annotations;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;

/** Conditional annotations for cloud platform specific beans. */
public interface CloudPlatform {

  /** The annotated type or method is conditionally enabled when the cloud platform is gcp. */
  @Target({ElementType.TYPE, ElementType.METHOD})
  @Retention(RetentionPolicy.RUNTIME)
  @ConditionalOnProperty(value = "env.wds.cloudPlatform", havingValue = "gcp")
  @interface GCP {}

  /** The annotated type or method is conditionally enabled when the cloud platform is azure. */
  @Target({ElementType.TYPE, ElementType.METHOD})
  @Retention(RetentionPolicy.RUNTIME)
  @ConditionalOnProperty(value = "env.wds.cloudPlatform", havingValue = "azure")
  @interface Azure {}
}
