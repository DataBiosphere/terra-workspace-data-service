package org.databiosphere.workspacedataservice.annotations;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import org.databiosphere.workspacedataservice.observability.TestObservationRegistryConfig;
import org.springframework.context.annotation.Import;

@Target(ElementType.TYPE)
@Retention(RetentionPolicy.RUNTIME)
@Import(TestObservationRegistryConfig.class)
public @interface WithTestObservationRegistry {}
