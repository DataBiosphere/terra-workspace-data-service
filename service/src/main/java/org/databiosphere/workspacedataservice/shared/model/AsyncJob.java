package org.databiosphere.workspacedataservice.shared.model;

import com.fasterxml.jackson.annotation.JsonInclude;

import java.time.LocalDateTime;

import static com.fasterxml.jackson.annotation.JsonInclude.Include.NON_NULL;

@JsonInclude(NON_NULL)
public class AsyncJob {

    private AsyncJobStatus status;
    private String jobId, errorMessage;
    private LocalDateTime created, updated;
    private Exception exception;
    private AsyncJobResult result;

    protected AsyncJob(AsyncJobStatus status, String jobId, String errorMessage, LocalDateTime created, LocalDateTime updated, Exception exception, AsyncJobResult result) {
        this.status = status;
        this.jobId = jobId;
        this.errorMessage = errorMessage;
        this.created = created;
        this.updated = updated;
        this.exception = exception;
        this.result = result;
    }

    // getters and setters

    public AsyncJobStatus getStatus() {
        return status;
    }

    public void setStatus(AsyncJobStatus status) {
        this.status = status;
    }

    public String getJobId() {
        return jobId;
    }

    public void setJobId(String jobId) {
        this.jobId = jobId;
    }

    public String getErrorMessage() {
        return errorMessage;
    }

    public void setErrorMessage(String errorMessage) {
        this.errorMessage = errorMessage;
    }

    public LocalDateTime getCreated() {
        return created;
    }

    public void setCreated(LocalDateTime created) {
        this.created = created;
    }

    public LocalDateTime getUpdated() {
        return updated;
    }

    public void setUpdated(LocalDateTime updated) {
        this.updated = updated;
    }

    public Exception getException() {
        return exception;
    }

    public void setException(Exception exception) {
        this.exception = exception;
    }

    public AsyncJobResult getResult() {
        return result;
    }

    public void setResult(AsyncJobResult result) {
        this.result = result;
    }
}
