package org.databiosphere.workspacedataservice.dataimport;

public record ImportRequirements(boolean protectedDataPolicy) {
  public ImportRequirements() {
    this(false);
  }
}
