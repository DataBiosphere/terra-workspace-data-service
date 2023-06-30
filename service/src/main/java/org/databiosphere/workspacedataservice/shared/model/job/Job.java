package org.databiosphere.workspacedataservice.shared.model.job;

import com.fasterxml.jackson.annotation.JsonInclude;

import java.time.LocalDateTime;
import java.util.UUID;

/**
 * Represents a long-running, probably asynchronous, process which moves
 * through multiple states before completing.
 * Different jobs can specify different JobResult implementations to
 * satisfy their own response payloads.
 *
 * @param <T> the response payload for this job.
 */
@JsonInclude(JsonInclude.Include.NON_NULL)
public class Job<T extends JobResult> {

    private UUID jobId;
    private JobStatus status;
    private String errorMessage;
    private LocalDateTime created, updated;
    private T result;

    public Job(UUID jobId, JobStatus status, String errorMessage, LocalDateTime created, LocalDateTime updated, T result) {
        this.jobId = jobId;
        this.status = status;
        this.errorMessage = errorMessage;
        this.created = created;
        this.updated = updated;
        this.result = result;
    }

    // getters and setters

    public JobStatus getStatus() {
        return status;
    }

    public void setStatus(JobStatus status) {
        this.status = status;
    }

    public UUID getJobId() {
        return jobId;
    }

    public void setJobId(UUID jobId) {
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

    public T getResult() {
        return result;
    }

    public void setResult(T result) {
        this.result = result;
    }
}