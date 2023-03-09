package org.databiosphere.workspacedataservice.sam;

import org.broadinstitute.dsde.workbench.client.sam.ApiException;
import org.databiosphere.workspacedataservice.service.model.exception.AuthenticationException;
import org.databiosphere.workspacedataservice.service.model.exception.AuthorizationException;
import org.databiosphere.workspacedataservice.service.model.exception.SamException;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.springframework.boot.test.context.SpringBootTest;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * Tests for @see HttpSamClientSupport
 */
@SpringBootTest
class HttpSamClientSupportTest extends HttpSamClientSupport {

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
                () -> executeSamRequest(samFunction, "AuthenticationException"),
                "samFunction should throw AuthenticationException");
        assertThrows(AuthenticationException.class,
                () -> executeSamRequest(voidSamFunction, "AuthenticationException"),
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
                () -> executeSamRequest(samFunction, "AuthorizationException"),
                "samFunction should throw AuthorizationException");
        assertThrows(AuthorizationException.class,
                () -> executeSamRequest(voidSamFunction, "AuthorizationException"),
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

    private void expectSamExceptionWithStatusCode(int expectedStatusCode, SamFunction<?> samFunction) {
        SamException actual = assertThrows(SamException.class,
                () -> executeSamRequest(samFunction, "samFunction-unittest"),
                "samFunction should throw SamException");

        assertEquals(expectedStatusCode, actual.getRawStatusCode(), "samFunction: Incorrect status code in SamException");
    }

    private void expectSamExceptionWithStatusCode(int expectedStatusCode, VoidSamFunction voidSamFunction) {
        SamException actual = assertThrows(SamException.class,
                () -> executeSamRequest(voidSamFunction, "samFunction-unittest"),
                "voidSamFunction should throw SamException");

        assertEquals(expectedStatusCode, actual.getRawStatusCode(), "voidSamFunction: Incorrect status code in SamException");
    }

}
