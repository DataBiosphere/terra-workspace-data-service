package org.databiosphere.workspacedataservice.dataimport;

import java.net.URI;
import org.databiosphere.workspacedataservice.config.DataImportProperties;
import org.databiosphere.workspacedataservice.generated.ImportRequestServerModel;
import org.databiosphere.workspacedataservice.service.model.exception.ValidationException;
import org.springframework.stereotype.Component;

@Component
public class ImportSourceValidator {
  private final DataImportProperties dataImportProperties;

  public ImportSourceValidator(DataImportProperties dataImportProperties) {
    this.dataImportProperties = dataImportProperties;
  }

  public void validateImportRequest(ImportRequestServerModel importRequest) {
    URI importUrl = importRequest.getUrl();

    if (!dataImportProperties.getAllowedSchemes().contains(importUrl.getScheme())) {
      throw new ValidationException(
          "Files may not be imported from %s URLs.".formatted(importUrl.getScheme()));
    }

    // File URLs don't have a host to validate.
    boolean isFileUrl = importUrl.getScheme().equals("file");
    if (!isFileUrl
        && dataImportProperties.getAllowedHosts().stream()
            .noneMatch(allowedHost -> allowedHost.matchesUrl(importUrl))) {
      throw new ValidationException(
          "Files may not be imported from %s.".formatted(importUrl.getHost()));
    }
  }
}
