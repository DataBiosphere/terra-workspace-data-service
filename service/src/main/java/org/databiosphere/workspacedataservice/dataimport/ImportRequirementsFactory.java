package org.databiosphere.workspacedataservice.dataimport;

import java.net.URI;
import java.util.List;
import org.databiosphere.workspacedataservice.config.DataImportProperties.ImportSourceConfig;

public class ImportRequirementsFactory {
  private final List<ImportSourceConfig> sources;

  public ImportRequirementsFactory(List<ImportSourceConfig> sources) {
    this.sources = sources;
  }

  public ImportRequirements getRequirementsForImport(URI importUri) {
    boolean requiresPrivateWorkspace =
        sources.stream()
            .filter(source -> source.matchesUri(importUri))
            .anyMatch(ImportSourceConfig::requirePrivateWorkspace);

    boolean requiresProtectedDataPolicy =
        sources.stream()
            .filter(source -> source.matchesUri(importUri))
            .anyMatch(ImportSourceConfig::requireProtectedDataPolicy);

    List<String> requiredAuthDomainGroups =
        sources.stream()
            .filter(source -> source.matchesUri(importUri))
            .flatMap(source -> source.requiredAuthDomainGroups().stream())
            .toList();

    return new ImportRequirements(
        requiresPrivateWorkspace, requiresProtectedDataPolicy, requiredAuthDomainGroups);
  }
}
