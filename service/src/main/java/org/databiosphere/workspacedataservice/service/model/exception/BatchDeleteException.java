package org.databiosphere.workspacedataservice.service.model.exception;

import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.ResponseStatus;

import java.util.List;

@ResponseStatus(code = HttpStatus.NOT_FOUND)
public class BatchDeleteException extends IllegalArgumentException {

	public BatchDeleteException(List<String> errorInfo) {
		super("We could not find some of the records you specified for deletion in your request. This list may not be exhaustive. " +
				"Be sure to look for similar errors. Here's a sampling of records we could not find: "
				+ errorInfo.subList(0, Math.min(errorInfo.size(), 100)));
	}

}
