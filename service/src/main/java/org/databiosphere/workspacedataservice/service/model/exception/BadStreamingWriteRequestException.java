package org.databiosphere.workspacedataservice.service.model.exception;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.ResponseStatus;

import java.io.IOException;

@ResponseStatus(code = HttpStatus.BAD_REQUEST)
public class BadStreamingWriteRequestException extends IllegalArgumentException {

	private static final Logger LOGGER = LoggerFactory.getLogger(BadStreamingWriteRequestException.class);
	public BadStreamingWriteRequestException(IOException ex) {
		super("The server doesn't understand the request. Please verify you are using "
				+ "the proper format.");
		LOGGER.error("error parsing request stream as json ", ex);
	}
}
