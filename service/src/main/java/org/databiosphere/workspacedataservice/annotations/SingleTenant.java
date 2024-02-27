package org.databiosphere.workspacedataservice.annotations;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import org.databiosphere.workspacedataservice.shared.model.WorkspaceId;
import org.springframework.beans.factory.annotation.Qualifier;

/**
 * Shorthand annotation for @Qualifier("singleTenant"), used to mark the {@link WorkspaceId} bean
 * used when running in single-tenant mode.
 */
@Target({ElementType.FIELD, ElementType.METHOD, ElementType.PARAMETER})
@Retention(RetentionPolicy.RUNTIME)
@Qualifier("singleTenant")
public @interface SingleTenant {}
