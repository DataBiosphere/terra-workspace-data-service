package org.databiosphere.workspacedataservice.dataimport;

import java.net.URI;
import java.util.List;
import java.util.stream.Stream;
import org.databiosphere.workspacedataservice.config.DataImportProperties.ImportSourceConfig;

public class ImportRequirementsFactory {
  private final List<ImportSourceConfig> sources;

  public ImportRequirementsFactory(List<ImportSourceConfig> sources) {
    this.sources = sources;
  }

  public ImportRequirements getRequirementsForImport(URI importUri) {
    Stream<ImportSourceConfig> matchingSources =
        sources.stream().filter(source -> source.matchesUri(importUri));

    boolean requiresProtectedDataPolicy =
        matchingSources
            .map(ImportSourceConfig::requireProtectedDataPolicy)
            .reduce(Boolean::logicalOr)
            .orElse(false);

    return new ImportRequirements(requiresProtectedDataPolicy);
  }
}
