package org.databiosphere.workspacedataservice.dataimport;

import java.util.List;

public record ImportRequirements(
    boolean privateWorkspace,
    boolean protectedDataPolicy,
    List<String> requiredAuthDomainGroups,
    boolean alwaysApplyAuthDomains) {
  public ImportRequirements() {
    this(false, false, List.of(), true);
  }
}
