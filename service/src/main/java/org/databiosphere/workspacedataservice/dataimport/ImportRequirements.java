package org.databiosphere.workspacedataservice.dataimport;

public record ImportRequirements(boolean privateWorkspace, boolean protectedDataPolicy) {
  public ImportRequirements() {
    this(false, false);
  }
}
