package org.databiosphere.workspacedataservice.service.model.exception;

import org.springframework.http.HttpStatus;

public class AuthenticationException extends SamException {

	public AuthenticationException(String message) {
		super(HttpStatus.UNAUTHORIZED, message);
	}
}
