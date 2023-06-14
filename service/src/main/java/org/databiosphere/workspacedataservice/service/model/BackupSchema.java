package org.databiosphere.workspacedataservice.service.model;

import java.sql.Timestamp;
import java.util.Date;
import java.util.UUID;

public class BackupSchema {
    public enum BackupState {
        Initiated,
        Started,
        InProgress,
        Completed,
        Error,
        Cancelled
    }

    public UUID id;
    public UUID sourceworkspaceid;
    public Timestamp createdtime;
    public Timestamp completedtime;
    public BackupState state;
    public String error;
    public String filename;

    public BackupSchema(UUID trackingId, UUID sourceWorkspaceId)
    {
        id = trackingId;
        sourceworkspaceid = sourceWorkspaceId;
        Date currentDate = new Date();
        createdtime = new Timestamp(currentDate.getTime());
        state = BackupState.Initiated;
    }

    public UUID getId() { return id; }
    public UUID getSourceworkspaceid() { return sourceworkspaceid; }
    public Timestamp getCreatedtime() { return createdtime; }
    public Timestamp getCompletedtime() { return completedtime; }
    public BackupState getState() { return state; }
    public String getError() { return error; }
    public String getFilename() { return filename; }

    public void setState(BackupState state) { this.state = state; }
    public void setFileName(String filename) { this.filename = filename; }
    public void setError(String error) {
        this.error = error; }

}
