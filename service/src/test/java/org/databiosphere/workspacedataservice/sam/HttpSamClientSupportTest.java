package org.databiosphere.workspacedataservice.sam;

import org.broadinstitute.dsde.workbench.client.sam.ApiException;
import org.databiosphere.workspacedataservice.retry.RetryLoggingListener;
import org.databiosphere.workspacedataservice.service.model.exception.AuthenticationException;
import org.databiosphere.workspacedataservice.service.model.exception.AuthorizationException;
import org.databiosphere.workspacedataservice.service.model.exception.SamException;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.retry.annotation.EnableRetry;
import org.springframework.test.annotation.DirtiesContext;

import java.util.concurrent.atomic.AtomicInteger;

import static org.databiosphere.workspacedataservice.sam.HttpSamClientSupport.SamFunction;
import static org.databiosphere.workspacedataservice.sam.HttpSamClientSupport.VoidSamFunction;
import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for @see HttpSamClientSupport
 */
@DirtiesContext
@SpringBootTest(classes = {SamConfig.class, RetryLoggingListener.class},
        properties = {"sam.retry.maxAttempts=2",
                "sam.retry.backoff.delay=10"}) // aggressive retry settings so unit test doesn't run too long
@EnableRetry
class HttpSamClientSupportTest {

    @Autowired HttpSamClientSupport httpSamClientSupport;

    @DisplayName("When Sam throws an ApiException with standard http status code 401, HttpSamClientSupport should throw AuthenticationException")
    @Test
    void authenticationException() {
        int samCode = 401;
        SamFunction<Boolean> samFunction = () -> {
            throw new ApiException(samCode, "");
        };
        VoidSamFunction voidSamFunction = () -> {
            throw new ApiException(samCode, "");
        };
        assertThrows(AuthenticationException.class,
                () -> httpSamClientSupport.withRetryAndErrorHandling(samFunction, "AuthenticationException"),
                "samFunction should throw AuthenticationException");
        assertThrows(AuthenticationException.class,
                () -> httpSamClientSupport.withRetryAndErrorHandling(voidSamFunction, "AuthenticationException"),
                "voidSamFunction should throw AuthenticationException");
    }

    @DisplayName("When Sam throws an ApiException with standard http status code 403, HttpSamClientSupport should throw AuthorizationException")
    @Test
    void authorizationException() {
        int samCode = 403;
        SamFunction<Boolean> samFunction = () -> {
            throw new ApiException(samCode, "");
        };
        VoidSamFunction voidSamFunction = () -> {
            throw new ApiException(samCode, "");
        };
        assertThrows(AuthorizationException.class,
                () -> httpSamClientSupport.withRetryAndErrorHandling(samFunction, "AuthorizationException"),
                "samFunction should throw AuthorizationException");
        assertThrows(AuthorizationException.class,
                () -> httpSamClientSupport.withRetryAndErrorHandling(voidSamFunction, "AuthorizationException"),
                "voidSamFunction should throw AuthorizationException");
    }

    @DisplayName("When Sam throws a NullPointerException, HttpSamClientSupport should throw SamException(500)")
    @Test
    void nullPointerException() {
        SamFunction<Boolean> samFunction = () -> {
            throw new NullPointerException();
        };
        VoidSamFunction voidSamFunction = () -> {
            throw new NullPointerException();
        };
        expectSamExceptionWithStatusCode(500, samFunction);
        expectSamExceptionWithStatusCode(500, voidSamFunction);
    }

    @DisplayName("When Sam throws a RuntimeException, HttpSamClientSupport should throw SamException(500)")
    @Test
    void runtimeException() {
        SamFunction<Boolean> samFunction = () -> {
            throw new RuntimeException();
        };
        VoidSamFunction voidSamFunction = () -> {
            throw new RuntimeException();
        };
        expectSamExceptionWithStatusCode(500, samFunction);
        expectSamExceptionWithStatusCode(500, voidSamFunction);
    }


    @ParameterizedTest(name = "When Sam throws an ApiException with nonstandard http status code {0}, HttpSamClientSupport should throw SamException with code 500")
    @ValueSource(ints = {0, -1, 8080})
    void apiExceptionsNonstandardCodes(int samCode) {
        SamFunction<Boolean> samFunction = () -> {
            throw new ApiException(samCode, "");
        };
        VoidSamFunction voidSamFunction = () -> {
            throw new ApiException(samCode, "");
        };
        expectSamExceptionWithStatusCode(500, samFunction);
        expectSamExceptionWithStatusCode(500, voidSamFunction);
    }

    @ParameterizedTest(name = "When Sam throws an ApiException with standard http status code {0}, HttpSamClientSupport should throw SamException with the same code")
    @ValueSource(ints = {400, 404, 500, 503})
    void apiExceptionsStandardCodes(int samCode) {
        SamFunction<Boolean> samFunction = () -> {
            throw new ApiException(samCode, "");
        };
        VoidSamFunction voidSamFunction = () -> {
            throw new ApiException(samCode, "");
        };
        expectSamExceptionWithStatusCode(samCode, samFunction);
        expectSamExceptionWithStatusCode(samCode, voidSamFunction);
    }


    @ParameterizedTest(name = "withSamErrorHandling will retry on status code {0}")
    @ValueSource(ints = {0, 500, 502, 503, 504})
    void retryableExceptions(int samCode) {
        AtomicInteger counter = new AtomicInteger(0);

        HttpSamClientSupport.SamFunction<Boolean> samFunction = () -> {
            counter.incrementAndGet();
            throw new ApiException(samCode, "");
        };

        // this test doesn't care what exception is thrown; other tests verify that
        assertThrows(Exception.class, () -> httpSamClientSupport.withRetryAndErrorHandling(samFunction, "retryableExceptions"));
        // with current settings, will retry 5 times. Any retry means we'll have more than
        // one invocation.
        assertTrue(counter.get() > 1, "SamFunction should have retried");

        // reset counter
        counter.set(0);

        HttpSamClientSupport.VoidSamFunction voidSamFunction = () -> {
            counter.incrementAndGet();
            throw new ApiException(samCode, "");
        };
        // this test doesn't care what exception is thrown; other tests verify that
        assertThrows(Exception.class, () -> httpSamClientSupport.withRetryAndErrorHandling(voidSamFunction, "retryableExceptions"));
        // with current settings, will retry 5 times. Any retry means we'll have more than
        // one invocation.
        assertTrue(counter.get() > 1, "VoidSamFunction should have retried");
    }

    @ParameterizedTest(name = "withSamErrorHandling will not retry on status code {0}")
    @ValueSource(ints = {400, 401, 403, 404, 409, 501})
    void nonRetryableExceptions(int samCode) {
        AtomicInteger counter = new AtomicInteger(0);

        HttpSamClientSupport.SamFunction<Boolean> samFunction = () -> {
            counter.incrementAndGet();
            throw new ApiException(samCode, "");
        };

        // this test doesn't care what exception is thrown; other tests verify that
        assertThrows(Exception.class, () -> httpSamClientSupport.withRetryAndErrorHandling(samFunction, "retryableExceptions"));
        // this test does care how many times the samFunction was invoked
        assertEquals(1, counter.get(), "SamFunction should not have retried");

        // reset counter
        counter.set(0);

        HttpSamClientSupport.VoidSamFunction voidSamFunction = () -> {
            counter.incrementAndGet();
            throw new ApiException(samCode, "");
        };
        // this test doesn't care what exception is thrown; other tests verify that
        assertThrows(Exception.class, () -> httpSamClientSupport.withRetryAndErrorHandling(voidSamFunction, "retryableExceptions"));
        // this test does care how many times the samFunction was invoked
        assertEquals(1, counter.get(), "VoidSamFunction should not have retried");
    }

    private void expectSamExceptionWithStatusCode(int expectedStatusCode, SamFunction<?> samFunction) {
        SamException actual = assertThrows(SamException.class,
                () -> httpSamClientSupport.withRetryAndErrorHandling(samFunction, "samFunction-unittest"),
                "samFunction should throw SamException");

        assertEquals(expectedStatusCode, actual.getRawStatusCode(), "samFunction: Incorrect status code in SamException");
    }

    private void expectSamExceptionWithStatusCode(int expectedStatusCode, VoidSamFunction voidSamFunction) {
        SamException actual = assertThrows(SamException.class,
                () -> httpSamClientSupport.withRetryAndErrorHandling(voidSamFunction, "samFunction-unittest"),
                "voidSamFunction should throw SamException");

        assertEquals(expectedStatusCode, actual.getRawStatusCode(), "voidSamFunction: Incorrect status code in SamException");
    }

}
