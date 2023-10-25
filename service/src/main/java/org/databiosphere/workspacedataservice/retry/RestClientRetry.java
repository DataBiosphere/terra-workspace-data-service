package org.databiosphere.workspacedataservice.retry;

import java.util.Objects;
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
import org.springframework.stereotype.Component;

@Component
public class RestClientRetry {
  private static final Logger LOGGER = LoggerFactory.getLogger(RestClientRetry.class);

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
    try {
      LOGGER.debug("Sending {} request to REST target ...", loggerHint);
      T functionResult = restCall.run();
      if (LOGGER.isTraceEnabled()) {
        LOGGER.trace("{} REST request successful, result: {}", loggerHint, functionResult);
      } else {
        LOGGER.debug("{} REST request successful", loggerHint);
      }
      return functionResult;
    } catch (Exception e) {
      int exceptionHttpCode = extractResponseCode(e);
      if (exceptionHttpCode == Integer.MIN_VALUE) {
        LOGGER.error(loggerHint + " REST request resulted in " + e.getMessage(), e);
        throw new RestException(
            HttpStatus.INTERNAL_SERVER_ERROR, "Error from REST target: " + e.getMessage());
      }

      LOGGER.error(
          loggerHint + " REST request resulted in ApiException(" + exceptionHttpCode + ")", e);
      if (exceptionHttpCode == 0) {
        throw new RestConnectionException();
      } else if (exceptionHttpCode == 401) {
        throw new AuthenticationException(e.getMessage());
      } else if (exceptionHttpCode == 403) {
        throw new AuthorizationException(e.getMessage());
      } else if (exceptionHttpCode == 500
          || exceptionHttpCode == 502
          || exceptionHttpCode == 503
          || exceptionHttpCode == 504) {
        throw new RestServerException(HttpStatus.resolve(exceptionHttpCode), e.getMessage());
      } else {
        HttpStatus resolvedStatus = HttpStatus.resolve(exceptionHttpCode);
        if (Objects.isNull(resolvedStatus)) {
          resolvedStatus = HttpStatus.INTERNAL_SERVER_ERROR;
        }
        throw new RestException(resolvedStatus, "Error from Sam: " + e.getMessage());
      }
    }
  }

  /**
   * Perform a Sam client request with logging and exception handling, if that Sam client request
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
    RestCall<String> wrappedFunction =
        () -> {
          voidRestCall.run();
          return "void";
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
    // WSM
    if (e instanceof bio.terra.workspace.client.ApiException wsmException) {
      return wsmException.getCode();
    }
    // TDR
    if (e instanceof bio.terra.datarepo.client.ApiException tdrException) {
      return tdrException.getCode();
    }
    // Leo
    if (e instanceof org.broadinstitute.dsde.workbench.client.leonardo.ApiException leoException) {
      return leoException.getCode();
    }
    // WDS
    if (e instanceof org.databiosphere.workspacedata.client.ApiException wdsException) {
      return wdsException.getCode();
    }
    return Integer.MIN_VALUE;
  }
}
