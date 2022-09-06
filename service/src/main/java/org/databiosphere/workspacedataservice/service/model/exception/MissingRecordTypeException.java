package org.databiosphere.workspacedataservice.service.model.exception;

import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.ResponseStatus;

@ResponseStatus(HttpStatus.NOT_FOUND)
public class MissingRecordTypeException extends RuntimeException {
	public MissingRecordTypeException(String message) {
		super(message);
	}
}
