package org.databiosphere.workspacedataservice.sam;

import org.broadinstitute.dsde.workbench.client.sam.ApiException;
import org.databiosphere.workspacedataservice.service.model.exception.AuthorizationException;
import org.databiosphere.workspacedataservice.service.model.exception.SamException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;

import java.util.Objects;

public abstract class HttpSamClientSupport {

    private static final Logger LOGGER = LoggerFactory.getLogger(HttpSamClientSupport.class);

    <T> T executeSamRequest(HttpSamDao.SamFunction<T> samFunction, String loggerHint) throws SamException, AuthorizationException {
        // TODO: add retries
        try {
            LOGGER.debug("Sending {} request to Sam ...", loggerHint);
            T functionResult = samFunction.run();
            LOGGER.debug("{} Sam request successful, result: {}", loggerHint, functionResult);
            return functionResult;
        } catch (ApiException apiException) {
            LOGGER.error("{} Sam request resulted in ApiException: {} {}",
                    loggerHint,
                    apiException.getCode(), apiException.getResponseBody());
            int code = apiException.getCode();
            switch (code) {
                case 401, 403 -> throw new AuthorizationException(apiException.getMessage());
                default -> {
                    HttpStatus resolvedStatus = HttpStatus.resolve(code);
                    if (Objects.isNull(resolvedStatus)) {
                        resolvedStatus = HttpStatus.INTERNAL_SERVER_ERROR;
                    }
                    throw new SamException(resolvedStatus, "Error from Sam: " + apiException.getMessage(), apiException);
                }
            }
        } catch (Exception e) {
            LOGGER.error("{} Sam request resulted in {}: {}", loggerHint, e.getClass().getName(), e.getMessage());
            throw new SamException(HttpStatus.INTERNAL_SERVER_ERROR, "Error from Sam: " + e.getMessage(), e);
        }
    }

    void executeSamRequest(HttpSamDao.VoidSamFunction voidSamFunction, String loggerHint) throws SamException, AuthorizationException {

        // wrap void function in something that returns an object
        HttpSamDao.SamFunction<String> wrappedFunction = () -> {
            voidSamFunction.run();
            return "void";
        };
        executeSamRequest(wrappedFunction, loggerHint);
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
