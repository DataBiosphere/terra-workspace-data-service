package org.databiosphere.workspacedataservice.shared.model.job;

/** The various types of Jobs that can be executed. */
public enum JobType {
  DATA_IMPORT, // async data import jobs
  WORKSPACE_INIT, // synchronous workspace initialization
  SYNC_BACKUP, // legacy synchronous backups
  SYNC_CLONE, // legacy synchronous clones
  SYNC_RESTORE // legacy synchronous restores
}
