package org.databiosphere.workspacedataservice.shared.model.job;

/** The various states a Job can move through. */
public enum JobType {
  DATAIMPORT, // async data import jobs
  SYNCBACKUP, // legacy synchronous backups
  SYNCCLONE, // legacy synchronous clones
  SYNCRESTORE // legacy synchronous restores
}
