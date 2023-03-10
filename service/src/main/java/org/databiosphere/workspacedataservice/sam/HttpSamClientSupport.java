package org.databiosphere.workspacedataservice.sam;

import org.broadinstitute.dsde.workbench.client.sam.ApiException;
import org.databiosphere.workspacedataservice.service.model.exception.AuthenticationException;
import org.databiosphere.workspacedataservice.service.model.exception.AuthorizationException;
import org.databiosphere.workspacedataservice.service.model.exception.SamException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;

import java.util.Objects;

/**
 * Utility/support functions for HttpSamClient:
 * - logging of all requests to Sam
 * - exception handling for Sam errors:
 *      - if we catch an ApiException with 401 http status from the Sam client,
 *          throw AuthenticationException
 *      - if we catch an ApiException with 403 http status from the Sam client,
 *          throw AuthorizationException
 *      - if we catch an ApiException with other well-known http status from the Sam client,
 *          throw SamException with the same status
 *      - if we catch an ApiException with non-parseable http status from the Sam client,
 *          throw SamException with status 500
 *      - if we catch a non-ApiException from the Sam client,
 *          throw SamException with status 500
 * - TODO: retry calls to Sam on retryable exceptions
 */
public abstract class HttpSamClientSupport {

    private static final Logger LOGGER = LoggerFactory.getLogger(HttpSamClientSupport.class);

    /**
     * Perform a Sam client request with logging and exception handling, and return the result of that request.
     *
     * @param samFunction the Sam client request to perform
     * @param loggerHint short string to include for all log entries for this request
     * @return the result of the Sam client request
     * @param <T> the return type of the Sam client request
     * @throws SamException on most exceptions thrown by the Sam client request
     * @throws AuthenticationException on a 401 from the Sam client request
     * @throws AuthorizationException on a 403 from the Sam client request
     */
    <T> T withSamErrorHandling(SamFunction<T> samFunction, String loggerHint) throws SamException, AuthenticationException, AuthorizationException {
        try {
            LOGGER.debug("Sending {} request to Sam ...", loggerHint);
            T functionResult = samFunction.run();
            LOGGER.debug("{} Sam request successful, result: {}", loggerHint, functionResult);
            return functionResult;
        } catch (ApiException apiException) {
            LOGGER.error(loggerHint + " Sam request resulted in ApiException(" + apiException.getCode() + ")",
                    apiException);
            int code = apiException.getCode();
            if (code == 401) {
                throw new AuthenticationException(apiException.getMessage());
            } else if (code == 403) {
                throw new AuthorizationException(apiException.getMessage());
            } else {
                HttpStatus resolvedStatus = HttpStatus.resolve(code);
                if (Objects.isNull(resolvedStatus)) {
                    resolvedStatus = HttpStatus.INTERNAL_SERVER_ERROR;
                }
                throw new SamException(resolvedStatus, "Error from Sam: " + apiException.getMessage());
            }
        } catch (Exception e) {
            LOGGER.error(loggerHint + " Sam request resulted in " + e.getMessage(), e);
            throw new SamException(HttpStatus.INTERNAL_SERVER_ERROR, "Error from Sam: " + e.getMessage());
        }
    }

    /**
     * Perform a Sam client request with logging and exception handling, if that Sam client request returns void.
     *
     * @param voidSamFunction the Sam client request to perform
     * @param loggerHint short string to include for all log entries for this request
     * @throws SamException on most exceptions thrown by the Sam client request
     * @throws AuthenticationException on a 401 from the Sam client request
     * @throws AuthorizationException on a 403 from the Sam client request
     */
    void withSamErrorHandling(VoidSamFunction voidSamFunction, String loggerHint) throws SamException, AuthenticationException, AuthorizationException {

        // wrap void function in something that returns an object
        SamFunction<String> wrappedFunction = () -> {
            voidSamFunction.run();
            return "void";
        };
        withSamErrorHandling(wrappedFunction, loggerHint);
    }

    /**
     * interface representing a callable Sam client function that returns a value.
     *
     * @param <T> return type of the Sam client function
     */
    @FunctionalInterface
    public interface SamFunction<T> {
        T run() throws Exception;
    }

    /**
     * interface representing a callable Sam client function that returns void.
     */
    @FunctionalInterface
    public interface VoidSamFunction {
        void run() throws Exception;
    }

}
