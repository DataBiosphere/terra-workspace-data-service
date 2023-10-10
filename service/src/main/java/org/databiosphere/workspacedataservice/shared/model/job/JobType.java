package org.databiosphere.workspacedataservice.shared.model.job;

/** The various types of Jobs that can be executed. */
public enum JobType {
  DATAIMPORT, // async data import jobs
  SYNCBACKUP, // legacy synchronous backups
  SYNCCLONE, // legacy synchronous clones
  SYNCRESTORE // legacy synchronous restores
}
