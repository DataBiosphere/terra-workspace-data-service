package org.databiosphere.workspacedataservice.service.model.exception;

import org.databiosphere.workspacedataservice.service.model.ReservedNames;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.ResponseStatus;

@ResponseStatus(code = HttpStatus.BAD_REQUEST, reason = "Attribute names can't start with "
		+ ReservedNames.RESERVED_NAME_PREFIX)
public class InvalidAttributeNameException extends RuntimeException {
}
