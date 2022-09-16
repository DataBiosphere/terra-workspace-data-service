package org.databiosphere.workspacedataservice.service.model.exception;

import org.databiosphere.workspacedataservice.service.model.ReservedNames;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.ResponseStatus;

@ResponseStatus(code = HttpStatus.BAD_REQUEST)
public class InvalidNameException extends RuntimeException {

	public enum NameType {
		ATTRIBUTE("Attribute"), RECORD_TYPE("Record Type");

		private final String name;

		NameType(String name) {
			this.name = name;
		}
	}
	public InvalidNameException(NameType nameType) {
		super(nameType.name + " names can't start with " + ReservedNames.RESERVED_NAME_PREFIX
				+ " or contain characters besides letters, numbers, spaces, dashes or underscores.");
	}
}
