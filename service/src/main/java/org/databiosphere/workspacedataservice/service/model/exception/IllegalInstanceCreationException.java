package org.databiosphere.workspacedataservice.service.model.exception;

import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.ResponseStatus;

import java.util.Arrays;

@ResponseStatus(HttpStatus.CONFLICT)
public class IllegalInstanceCreationException extends RuntimeException {
	public IllegalInstanceCreationException(String message) {
		super(message);
	}
}
