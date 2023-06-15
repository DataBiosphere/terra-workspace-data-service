package org.databiosphere.workspacedataservice.shared.model;

public class BackupResult implements AsyncJobResult {

    private final String backupFile;

    public BackupResult(String backupFile) {
        this.backupFile = backupFile;
    }
}
