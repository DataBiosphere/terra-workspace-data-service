package org.databiosphere.workspacedataservice.shared.model;

import java.time.LocalDateTime;

public class AsyncJobBuilder {
    private AsyncJobStatus status;
    private String jobId;
    private String errorMessage;
    private LocalDateTime created;
    private LocalDateTime updated;
    private Exception exception;
    private AsyncJobResult result;

    public AsyncJobBuilder(String jobId) {
        this.jobId = jobId;
    }

    public AsyncJobBuilder withStatus(AsyncJobStatus status) {
        this.status = status;
        return this;
    }

    public AsyncJobBuilder withErrorMessage(String errorMessage) {
        this.errorMessage = errorMessage;
        return this;
    }

    public AsyncJobBuilder withCreated(LocalDateTime created) {
        this.created = created;
        return this;
    }

    public AsyncJobBuilder withUpdated(LocalDateTime updated) {
        this.updated = updated;
        return this;
    }

    public AsyncJobBuilder withException(Exception exception) {
        this.exception = exception;
        return this;
    }

    public AsyncJobBuilder withResult(AsyncJobResult result) {
        this.result = result;
        return this;
    }

    public AsyncJob build() {
        return new AsyncJob(status, jobId, errorMessage, created, updated, exception, result);
    }
}