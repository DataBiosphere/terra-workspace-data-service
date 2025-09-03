package org.databiosphere.workspacedataservice.dataimport;

import java.net.URI;
import java.util.List;
import java.util.Set;
import java.util.regex.Pattern;
import org.databiosphere.workspacedataservice.config.DataImportProperties.ImportSourceConfig;

public class ImportRequirementsFactory {
  private final List<ImportSourceConfig> sources;
  private final Set<Pattern> allowlist;

  public ImportRequirementsFactory(
      List<ImportSourceConfig> sources, Set<Pattern> allowlistPatterns) {
    this.sources = sources;
    this.allowlist = allowlistPatterns;
  }

  public ImportRequirements getRequirementsForImport(URI importUri) {
    boolean isAllowlisted =
        allowlist.stream().anyMatch(pattern -> pattern.matcher(importUri.toString()).matches());

    if (isAllowlisted) {
      return new ImportRequirements(false, false, List.of());
    }

    // TODO don't match uri here
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
