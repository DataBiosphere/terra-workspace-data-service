package org.databiosphere.workspacedataservice.service.model.exception;

import org.databiosphere.workspacedataservice.shared.model.RecordType;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.ResponseStatus;

@ResponseStatus(code = HttpStatus.BAD_REQUEST)
public class NewPrimaryKeyException extends RuntimeException {


	public NewPrimaryKeyException(String oldPk, String newPk, RecordType recordType) {
		super("The primary key for " + recordType + " is already set to " + oldPk
				+ " if you wish to change it to " + newPk + " you'll need to create a new record type.");
	}
}
