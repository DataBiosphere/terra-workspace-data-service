package org.databiosphere.workspacedataservice.dataimport;

import java.net.URI;
import java.util.List;
import java.util.regex.Pattern;
import org.databiosphere.workspacedataservice.config.DataImportProperties;
import org.databiosphere.workspacedataservice.config.DataImportProperties.ImportSourceConfig;
import org.springframework.stereotype.Component;

@Component
public class ImportRequirementsFactory {
  private final List<ImportSourceConfig> sources;

  public ImportRequirementsFactory(DataImportProperties dataImportProperties) {
    this.sources = dataImportProperties.getSources();
  }

  public ImportRequirements getRequirementsForImport(URI importUri) {
    boolean requiresProtectedDataPolicy = false;

    for (ImportSourceConfig source : sources) {
      for (Pattern urlPattern : source.urls()) {
        if (urlPattern.matcher(importUri.toString()).find()) {
          if (source.requireProtectedDataPolicy()) {
            requiresProtectedDataPolicy = true;
          }
          break;
        }
      }
    }

    return new ImportRequirements(requiresProtectedDataPolicy);
  }
}
