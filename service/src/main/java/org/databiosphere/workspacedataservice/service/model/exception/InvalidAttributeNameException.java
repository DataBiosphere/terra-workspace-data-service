package org.databiosphere.workspacedataservice.service.model.exception;

import org.databiosphere.workspacedataservice.service.model.ReservedNames;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.ResponseStatus;

@ResponseStatus(HttpStatus.BAD_REQUEST)
public class InvalidAttributeNameException extends RuntimeException {

	public InvalidAttributeNameException() {
		super("Attribute names can't start with " + ReservedNames.RESERVED_NAME_PREFIX);
	}
}
