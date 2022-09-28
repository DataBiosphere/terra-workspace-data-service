package org.databiosphere.workspacedataservice.service.model.exception;

import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.ResponseStatus;

import java.util.Map;

@ResponseStatus(code = HttpStatus.NOT_FOUND)
public class BatchDeleteException extends IllegalArgumentException {

	public BatchDeleteException(Map<String, String> errorInfo) {
		super("We could not find some of the records you specified for deletion in your request. Here's a sampling of records we could not find: "
				+ errorInfo);
	}

}
