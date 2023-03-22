package org.databiosphere.workspacedataservice.controller;

import org.springframework.retry.annotation.Backoff;
import org.springframework.retry.annotation.Retryable;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Annotation to indicate the the entire implementation of a WDS API can be retried.
 * The API must be idempotent. This implies that if the API performs any database writes,
 * such writes are encapsulated in a transaction and the transaction is rolled back
 * on an exception.
 *
 * Some database errors are not retry-able via the lower-level retries
 * in @ReadTransaction and @WriteTransaction. This higher-level @RetryableApi
 * annotation allows retries to occur at the whole-API level.
 */
@Target(ElementType.METHOD)
@Retention(RetentionPolicy.RUNTIME)
@Retryable(maxAttemptsExpression = "${api.retry.maxAttempts}",
        backoff = @Backoff(delayExpression = "${api.retry.backoff.delay}",
                multiplierExpression = "${api.retry.backoff.multiplier}"))
public @interface RetryableApi {}
