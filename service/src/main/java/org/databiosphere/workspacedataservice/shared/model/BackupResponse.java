package org.databiosphere.workspacedataservice.shared.model;

public record BackupResponse(boolean backupStatus, String state, String filename, String message) {

}
