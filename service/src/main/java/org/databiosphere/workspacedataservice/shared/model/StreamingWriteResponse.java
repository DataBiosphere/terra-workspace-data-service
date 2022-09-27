package org.databiosphere.workspacedataservice.shared.model;

import org.databiosphere.workspacedataservice.service.model.exception.BatchWriteException;

import java.util.List;

public class StreamingWriteResponse {

    private final List<BatchWriteException> errors;

    private final String errorMessage;

    public StreamingWriteResponse(List<BatchWriteException> errors, String errorMessage) {
        this.errors = errors;
        this.errorMessage = errorMessage;
    }

    public List<BatchWriteException> getErrors() {
        return errors;
    }

    public String getErrorMessage() {
        return errorMessage;
    }
}
