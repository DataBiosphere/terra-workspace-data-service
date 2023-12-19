package org.databiosphere.workspacedataservice.retry;

import static org.junit.jupiter.api.Assertions.*;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import java.lang.reflect.InvocationTargetException;
import java.util.concurrent.atomic.AtomicInteger;
import org.databiosphere.workspacedataservice.retry.RestClientRetry.RestCall;
import org.databiosphere.workspacedataservice.retry.RestClientRetry.VoidRestCall;
import org.databiosphere.workspacedataservice.service.model.exception.AuthenticationException;
import org.databiosphere.workspacedataservice.service.model.exception.AuthorizationException;
import org.databiosphere.workspacedataservice.service.model.exception.RestException;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.junitpioneer.jupiter.cartesian.CartesianTest;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.retry.annotation.EnableRetry;
import org.springframework.test.annotation.DirtiesContext;

/** Tests for @see RestClientRetry */
@DirtiesContext
@SpringBootTest(
    classes = {RestClientRetry.class, RetryLoggingListener.class},
    properties = {
      "rest.retry.maxAttempts=2",
      "rest.retry.backoff.delay=10"
    }) // aggressive retry settings so unit test doesn't run too long
@EnableRetry
class RestClientRetryTest {

  @Autowired RestClientRetry restClientRetry;

  /**
   * reusable annotation for @CartesianTest that supplies all the exceptions we care about.
   * Necessary because the array of classes must be a constant.
   */
  @Retention(RetentionPolicy.RUNTIME)
  @Target({ElementType.PARAMETER, ElementType.ANNOTATION_TYPE})
  @CartesianTest.Values(
      classes = {
        org.broadinstitute.dsde.workbench.client.sam.ApiException.class,
        bio.terra.workspace.client.ApiException.class,
        bio.terra.datarepo.client.ApiException.class,
        org.broadinstitute.dsde.workbench.client.leonardo.ApiException.class,
        org.databiosphere.workspacedata.client.ApiException.class
      })
  @interface CartesianTestableExceptions {}

  /**
   * reusable annotation for @ParameterizedTest that supplies all the exceptions we care about.
   * Necessary because the array of classes must be a constant.
   */
  @Target({ElementType.ANNOTATION_TYPE, ElementType.METHOD})
  @Retention(RetentionPolicy.RUNTIME)
  @ValueSource(
      classes = {
        org.broadinstitute.dsde.workbench.client.sam.ApiException.class,
        bio.terra.workspace.client.ApiException.class,
        bio.terra.datarepo.client.ApiException.class,
        org.broadinstitute.dsde.workbench.client.leonardo.ApiException.class,
        org.databiosphere.workspacedata.client.ApiException.class
      })
  @interface TestableExceptionsSource {}

  @ParameterizedTest(
      name =
          "When REST target throws a {0} with standard http status code 401, restClientRetry should throw AuthenticationException")
  @TestableExceptionsSource
  void authenticationException(Class<Exception> clazz)
      throws NoSuchMethodException,
          InvocationTargetException,
          InstantiationException,
          IllegalAccessException {
    int code = 401;
    Exception apiException =
        clazz.getDeclaredConstructor(int.class, String.class).newInstance(code, "");
    RestCall<Boolean> RestCall =
        () -> {
          throw apiException;
        };
    VoidRestCall voidRestCall =
        () -> {
          throw apiException;
        };
    assertThrows(
        AuthenticationException.class,
        () -> restClientRetry.withRetryAndErrorHandling(RestCall, "AuthenticationException"),
        "RestCall should throw AuthenticationException");
    assertThrows(
        AuthenticationException.class,
        () -> restClientRetry.withRetryAndErrorHandling(voidRestCall, "AuthenticationException"),
        "voidRestCall should throw AuthenticationException");
  }

  @ParameterizedTest(
      name =
          "When REST target throws a {0} with standard http status code 403, restClientRetry should throw AuthorizationException")
  @TestableExceptionsSource
  void authorizationException(Class<Exception> clazz)
      throws NoSuchMethodException,
          InvocationTargetException,
          InstantiationException,
          IllegalAccessException {
    int code = 403;
    Exception apiException =
        clazz.getDeclaredConstructor(int.class, String.class).newInstance(code, "");
    RestCall<Boolean> RestCall =
        () -> {
          throw apiException;
        };
    VoidRestCall voidRestCall =
        () -> {
          throw apiException;
        };
    assertThrows(
        AuthorizationException.class,
        () -> restClientRetry.withRetryAndErrorHandling(RestCall, "AuthorizationException"),
        "RestCall should throw AuthorizationException");
    assertThrows(
        AuthorizationException.class,
        () -> restClientRetry.withRetryAndErrorHandling(voidRestCall, "AuthorizationException"),
        "voidRestCall should throw AuthorizationException");
  }

  @DisplayName(
      "When REST target throws a NullPointerException, restClientRetry should throw RestException(500)")
  @Test
  void nullPointerException() {
    RestCall<Boolean> RestCall =
        () -> {
          throw new NullPointerException();
        };
    VoidRestCall voidRestCall =
        () -> {
          throw new NullPointerException();
        };
    expectRestExceptionWithStatusCode(500, RestCall);
    expectRestExceptionWithStatusCode(500, voidRestCall);
  }

  @DisplayName(
      "When REST target throws a RuntimeException, restClientRetry should throw RestException(500)")
  @Test
  void runtimeException() {
    RestCall<Boolean> RestCall =
        () -> {
          throw new RuntimeException();
        };
    VoidRestCall voidRestCall =
        () -> {
          throw new RuntimeException();
        };
    expectRestExceptionWithStatusCode(500, RestCall);
    expectRestExceptionWithStatusCode(500, voidRestCall);
  }

  @CartesianTest(
      name =
          "When REST target throws a {1} with nonstandard http status code {0}, restClientRetry should throw RestException with code 500")
  void apiExceptionsNonstandardCodes(
      @CartesianTest.Values(ints = {0, -1, 8080}) int code,
      @CartesianTestableExceptions Class<Exception> clazz)
      throws NoSuchMethodException,
          InvocationTargetException,
          InstantiationException,
          IllegalAccessException {
    Exception apiException =
        clazz.getDeclaredConstructor(int.class, String.class).newInstance(code, "");
    RestCall<Boolean> RestCall =
        () -> {
          throw apiException;
        };
    VoidRestCall voidRestCall =
        () -> {
          throw apiException;
        };
    expectRestExceptionWithStatusCode(500, RestCall);
    expectRestExceptionWithStatusCode(500, voidRestCall);
  }

  @CartesianTest(
      name =
          "When REST target throws a {1} with standard http status code {0}, restClientRetry should throw RestException with the same code")
  void apiExceptionsStandardCodes(
      @CartesianTest.Values(ints = {400, 404, 500, 503}) int code,
      @CartesianTestableExceptions Class<Exception> clazz)
      throws NoSuchMethodException,
          InvocationTargetException,
          InstantiationException,
          IllegalAccessException {
    Exception apiException =
        clazz.getDeclaredConstructor(int.class, String.class).newInstance(code, "");

    RestCall<Boolean> RestCall =
        () -> {
          throw apiException;
        };
    VoidRestCall voidRestCall =
        () -> {
          throw apiException;
        };
    expectRestExceptionWithStatusCode(code, RestCall);
    expectRestExceptionWithStatusCode(code, voidRestCall);
  }

  @CartesianTest(name = "withRetryAndErrorHandling will retry on status code {0} for exception {1}")
  void retryableExceptions(
      @CartesianTest.Values(ints = {0, 500, 502, 503, 504}) int code,
      @CartesianTestableExceptions Class<Exception> clazz)
      throws NoSuchMethodException,
          InvocationTargetException,
          InstantiationException,
          IllegalAccessException {
    AtomicInteger counter = new AtomicInteger(0);
    Exception apiException =
        clazz.getDeclaredConstructor(int.class, String.class).newInstance(code, "");

    RestCall<Boolean> RestCall =
        () -> {
          counter.incrementAndGet();
          throw apiException;
        };

    // this test doesn't care what exception is thrown; other tests verify that
    assertThrows(
        Exception.class,
        () -> restClientRetry.withRetryAndErrorHandling(RestCall, "retryableExceptions"));
    // with current settings, will retry 5 times. Any retry means we'll have more than
    // one invocation.
    assertTrue(counter.get() > 1, "RestCall should have retried");

    // reset counter
    counter.set(0);

    VoidRestCall voidRestCall =
        () -> {
          counter.incrementAndGet();
          throw apiException;
        };
    // this test doesn't care what exception is thrown; other tests verify that
    assertThrows(
        Exception.class,
        () -> restClientRetry.withRetryAndErrorHandling(voidRestCall, "retryableExceptions"));
    // with current settings, will retry 5 times. Any retry means we'll have more than
    // one invocation.
    assertTrue(counter.get() > 1, "VoidRestCall should have retried");
  }

  @CartesianTest(
      name = "withRetryAndErrorHandling will not retry on status code {0} for exception {1}")
  void nonRetryableExceptions(
      @CartesianTest.Values(ints = {400, 401, 403, 404, 409, 501}) int code,
      @CartesianTestableExceptions Class<Exception> clazz)
      throws NoSuchMethodException,
          InvocationTargetException,
          InstantiationException,
          IllegalAccessException {
    AtomicInteger counter = new AtomicInteger(0);

    Exception apiException =
        clazz.getDeclaredConstructor(int.class, String.class).newInstance(code, "");

    RestCall<Boolean> RestCall =
        () -> {
          counter.incrementAndGet();
          throw apiException;
        };

    // this test doesn't care what exception is thrown; other tests verify that
    assertThrows(
        Exception.class,
        () -> restClientRetry.withRetryAndErrorHandling(RestCall, "retryableExceptions"));
    // this test does care how many times the RestCall was invoked
    assertEquals(1, counter.get(), "RestCall should not have retried");

    // reset counter
    counter.set(0);

    VoidRestCall voidRestCall =
        () -> {
          counter.incrementAndGet();
          throw apiException;
        };
    // this test doesn't care what exception is thrown; other tests verify that
    assertThrows(
        Exception.class,
        () -> restClientRetry.withRetryAndErrorHandling(voidRestCall, "retryableExceptions"));
    // this test does care how many times the RestCall was invoked
    assertEquals(1, counter.get(), "VoidRestCall should not have retried");
  }

  private void expectRestExceptionWithStatusCode(int expectedStatusCode, RestCall<?> RestCall) {
    RestException actual =
        assertThrows(
            RestException.class,
            () -> restClientRetry.withRetryAndErrorHandling(RestCall, "RestCall-unittest"),
            "RestCall should throw RestException");

    assertEquals(
        expectedStatusCode,
        actual.getStatusCode().value(),
        "RestCall: Incorrect status code in RestException");
  }

  private void expectRestExceptionWithStatusCode(
      int expectedStatusCode, VoidRestCall voidRestCall) {
    RestException actual =
        assertThrows(
            RestException.class,
            () -> restClientRetry.withRetryAndErrorHandling(voidRestCall, "RestCall-unittest"),
            "voidRestCall should throw RestException");

    assertEquals(
        expectedStatusCode,
        actual.getStatusCode().value(),
        "voidRestCall: Incorrect status code in RestException");
  }
}
