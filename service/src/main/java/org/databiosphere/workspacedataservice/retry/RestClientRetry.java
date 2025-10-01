package org.databiosphere.workspacedataservice.retry;

import static java.util.Objects.requireNonNull;

import io.micrometer.observation.Observation;
import io.micrometer.observation.ObservationRegistry;
import java.util.Objects;
import java.util.Optional;
import org.databiosphere.workspacedataservice.service.model.exception.AuthenticationException;
import org.databiosphere.workspacedataservice.service.model.exception.AuthorizationException;
import org.databiosphere.workspacedataservice.service.model.exception.RestConnectionException;
import org.databiosphere.workspacedataservice.service.model.exception.RestException;
import org.databiosphere.workspacedataservice.service.model.exception.RestRetryableException;
import org.databiosphere.workspacedataservice.service.model.exception.RestServerException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;
import org.springframework.retry.annotation.Backoff;
import org.springframework.retry.annotation.Retryable;
import org.springframework.retry.support.RetrySynchronizationManager;
import org.springframework.stereotype.Component;
import org.springframework.web.client.HttpStatusCodeException;

@Component
public class RestClientRetry {
  private static final Logger LOGGER = LoggerFactory.getLogger(RestClientRetry.class);
  private final ObservationRegistry observations;

  public RestClientRetry(ObservationRegistry observations) {
    this.observations = observations;
  }

  /**
   * Perform a REST client request with logging and exception handling, and return the result of
   * that request.
   *
   * @param restCall the REST client request to perform
   * @param loggerHint short string to include for all log entries for this request
   * @return the result of the REST client request
   * @param <T> the return type of the REST client request
   * @throws RestException on most exceptions thrown by the REST client request
   * @throws AuthenticationException on a 401 from the REST client request
   * @throws AuthorizationException on a 403 from the REST client request
   */
  @Retryable(
      include = {RestRetryableException.class},
      maxAttemptsExpression = "${rest.retry.maxAttempts}",
      backoff =
          @Backoff(
              delayExpression = "${rest.retry.backoff.delay}",
              multiplierExpression = "${rest.retry.backoff.multiplier}"),
      listeners = {"retryLoggingListener"})
  public <T> T withRetryAndErrorHandling(RestCall<T> restCall, String loggerHint)
      throws RestException, AuthenticationException, AuthorizationException {
    Observation observation =
        Observation.start("wds.outbound", observations).lowCardinalityKeyValue("hint", loggerHint);
    try {
      LOGGER.debug("Sending {} request to REST target ...", loggerHint);
      T functionResult = restCall.run();
      if (LOGGER.isTraceEnabled()) {
        LOGGER.trace("{} REST request successful, result: {}", loggerHint, functionResult);
      } else {
        LOGGER.debug("{} REST request successful", loggerHint);
      }
      observation.lowCardinalityKeyValue("outcome", "SUCCEEDED");
      return functionResult;
    } catch (Exception e) {
      int exceptionHttpCode = extractResponseCode(e);
      observation
          .lowCardinalityKeyValue("responseCode", String.valueOf(exceptionHttpCode))
          .lowCardinalityKeyValue("outcome", "ERROR");
      observation.error(e);
      switch (exceptionHttpCode) {
        // retryable http codes
        case 0:
          LOGGER.warn(loggerHint + " REST request resulted in connection failure", e);
          throw new RestConnectionException();
        case 500:
        case 502:
        case 503:
        case 504:
          LOGGER.warn(
              loggerHint + " REST request resulted in ApiException(" + exceptionHttpCode + ")", e);
          throw new RestServerException(
              requireNonNull(HttpStatus.resolve(exceptionHttpCode)), e.getMessage()); // retryable

        // non-retryable http codes
        case 401:
          throw new AuthenticationException(e.getMessage());
        case 403:
          throw new AuthorizationException(e.getMessage());
        default:
          HttpStatus resolvedStatus = HttpStatus.resolve(exceptionHttpCode);
          if (Objects.isNull(resolvedStatus)) {
            resolvedStatus = HttpStatus.INTERNAL_SERVER_ERROR;
          }
          throw new RestException(
              resolvedStatus,
              "Error from " + loggerHint + " REST target: " + e.getMessage()); // not retryable
      }
    } finally {
      Optional.ofNullable(RetrySynchronizationManager.getContext())
          .ifPresent(
              context -> {
                int retryCount = context.getRetryCount();
                if (retryCount > 0) {
                  observation.lowCardinalityKeyValue("retryCount", Integer.toString(retryCount));
                }
              });
      observation.stop();
    }
  }

  /**
   * Perform a REST client request with logging and exception handling, if that REST client request
   * returns void.
   *
   * @param voidRestCall the REST client request to perform
   * @param loggerHint short string to include for all log entries for this request
   * @throws RestException on most exceptions thrown by the REST client request
   * @throws AuthenticationException on a 401 from the REST client request
   * @throws AuthorizationException on a 403 from the REST client request
   */
  @Retryable(
      include = {RestRetryableException.class},
      maxAttemptsExpression = "${rest.retry.maxAttempts}",
      backoff =
          @Backoff(
              delayExpression = "${rest.retry.backoff.delay}",
              multiplierExpression = "${rest.retry.backoff.multiplier}"))
  public void withRetryAndErrorHandling(VoidRestCall voidRestCall, String loggerHint)
      throws RestException, AuthenticationException, AuthorizationException {

    // wrap void function in something that returns an object
    RestCall<?> wrappedFunction =
        () -> {
          voidRestCall.run();
          return null;
        };
    withRetryAndErrorHandling(wrappedFunction, loggerHint);
  }

  /**
   * interface representing a callable REST client function that returns a value.
   *
   * @param <T> return type of the REST client function
   */
  @FunctionalInterface
  public interface RestCall<T> {
    T run() throws Exception;
  }

  /** interface representing a callable REST client function that returns void. */
  @FunctionalInterface
  public interface VoidRestCall {
    void run() throws Exception;
  }

  /* Utility method to extract the HTTP response code for a REST call failure out of an
     Exception. Because our various REST clients use autogenerated classes, and throw
     autogenerated Exceptions, there is no single reliable superclass other than
     Exception.

     We could try to do something fancy with reflection and generics, but the boilerplate
     code below seems the best approach: it's readable and performant, and we don't
     add new REST clients that often.
  */
  private int extractResponseCode(Exception e) {
    // Sam
    if (e instanceof org.broadinstitute.dsde.workbench.client.sam.ApiException samException) {
      return samException.getCode();
    }
    // TDR
    if (e instanceof bio.terra.datarepo.client.ApiException tdrException) {
      return tdrException.getCode();
    }
    // Rawls, or anything using RestTemplate
    if (e instanceof HttpStatusCodeException httpStatusCodeException) {
      return httpStatusCodeException.getStatusCode().value();
    }

    return Integer.MIN_VALUE;
  }
}
