package org.databiosphere.workspacedataservice.service.model.exception;

import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.ResponseStatus;
import org.springframework.web.server.ResponseStatusException;

public class SamException extends ResponseStatusException {

	public SamException(HttpStatus status, String message) {
		super(status, message);
	}

	public SamException(HttpStatus status, String message, Throwable cause) {
		super(status, message, cause);
	}
}
