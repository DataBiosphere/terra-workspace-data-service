package org.databiosphere.workspacedataservice.service.model.exception;

import org.springframework.http.HttpStatus;

/**
 * Indicates a failure to connect to Sam, typically a timeout, and generally due to
 * transient network or connection issues.
 */
public class SamConnectionException extends SamRetryableException {

    public SamConnectionException() {
        super(HttpStatus.INTERNAL_SERVER_ERROR, "Could not connect to Sam");
    }
    public SamConnectionException(HttpStatus status, String message) {
        super(status, message);
    }
}
