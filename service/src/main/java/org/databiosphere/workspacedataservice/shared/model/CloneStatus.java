package org.databiosphere.workspacedataservice.shared.model;

/**
 * The various states encountered during cloning.
 */
public enum CloneStatus {
    UNKNOWN, // in case it cant be retrieved
    BACKUPQUEUED,     // backup job has been created but not yet started
    BACKUPSUCCEEDED,  // backup job completed as expected
    BACKUPERROR      // backup job failed
}
