package org.databiosphere.workspacedataservice.service.model;

import org.databiosphere.workspacedataservice.shared.model.BackupRequest;
import org.databiosphere.workspacedataservice.shared.model.job.JobStatus;

import java.sql.Timestamp;
import java.time.Instant;
import java.util.UUID;

public class BackupSchema {

    private UUID id;
    private JobStatus state;
    private String error;
    private Timestamp createdtime;
    private Timestamp updatedtime;
    private UUID requester;
    private String description;
    private String filename;

    public BackupSchema() {
    }

    public BackupSchema(UUID trackingId, BackupRequest backupRequest) {
        this.id = trackingId;
        Timestamp now = Timestamp.from(Instant.now());
        this.createdtime = now;
        this.updatedtime = now;
        this.state = JobStatus.INITIATED;
        this.requester = backupRequest.requestingWorkspaceId();
        this.description = backupRequest.description();
    }

    public UUID getId() {
        return id;
    }

    public Timestamp getCreatedtime() {
        return createdtime;
    }

    public Timestamp getUpdatedtime() {
        return updatedtime;
    }

    public JobStatus getState() {
        return state;
    }

    public String getError() {
        return error;
    }

    public String getFilename() {
        return filename;
    }

    public UUID getRequester() {
        return requester;
    }

    public String getDescription() {
        return description;
    }

    public void setCreatedtime(Timestamp createdtime) {
        this.createdtime = createdtime;
    }

    public void setUpdatedtime(Timestamp updatedtime) {
        this.updatedtime = updatedtime;
    }

    public void setRequester(UUID requester) {
        this.requester = requester;
    }

    public void setDescription(String description) {
        this.description = description;
    }

    public void setFilename(String filename) {
        this.filename = filename;
    }

    public void setState(JobStatus state) {
        this.state = state;
    }

    public void setFileName(String filename) {
        this.filename = filename;
    }

    public void setError(String error) {
        this.error = error;
    }

    public void setId(UUID id) {
        this.id = id;
    }
}