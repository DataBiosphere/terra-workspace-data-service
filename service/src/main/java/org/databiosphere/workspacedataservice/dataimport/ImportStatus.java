package org.databiosphere.workspacedataservice.dataimport;

/** Enum representing the different states a WDS data import can be in. */
public enum ImportStatus {
  CREATED("Created"), // written to the WDS db, but not yet written to Quartz
  QUEUED("Queued"), // written to WDS db AND to Quartz
  RUNNING("Running"), // running in Quartz
  SUCCEEDED("Succeeded"), // finished ok in Quartz
  FAILED("Failed"), // hit error in Quartz
  UNKNOWN("Unknown"); // catch-all; we should never hit this

  private final String name;

  public String getName() {
    return name;
  }

  ImportStatus(String name) {
    this.name = name;
  }
}
