package org.databiosphere.workspacedataservice.service.model;

import java.sql.Timestamp;
import java.util.Date;
import java.util.UUID;

public class BackupSchema {
    public enum BackupState {
        INITIATED,
        STARTED,
        INPROGRESS,
        COMPLETED,
        ERROR,
        CANCELLED
    }

    private UUID id;
    private Timestamp createdtime;
    private Timestamp completedtime;
    private BackupState state;
    private String error;
    private String filename;

    public BackupSchema() {}

    public BackupSchema(UUID trackingId)
    {
        this.id = trackingId;
        Date currentDate = new Date();
        this.createdtime = new Timestamp(currentDate.getTime());
        this.state = BackupState.INITIATED;
    }

    public UUID getId() { return id; }
    public Timestamp getCreatedtime() { return createdtime; }
    public Timestamp getCompletedtime() { return completedtime; }
    public BackupState getState() { return state; }
    public String getError() { return error; }
    public String getFilename() { return filename; }

    public void setState(BackupState state) { this.state = state; }
    public void setFileName(String filename) { this.filename = filename; }
    public void setError(String error) { this.error = error; }
    public void setId(UUID id) { this.id = id; }

    public void setCompletedtime() {
        Date currentDate = new Date();
        this.completedtime = new Timestamp(currentDate.getTime());
    }
}
