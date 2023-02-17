package org.databiosphere.workspacedataservice.service.model.exception;

import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.ResponseStatus;

@ResponseStatus(code = HttpStatus.INTERNAL_SERVER_ERROR)
public class UnexpectedTsvException extends IllegalArgumentException {

	public UnexpectedTsvException(String message) {
		super(message);
	}
}
