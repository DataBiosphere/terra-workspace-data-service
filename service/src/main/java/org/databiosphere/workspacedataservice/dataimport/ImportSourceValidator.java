package org.databiosphere.workspacedataservice.dataimport;

import java.net.URI;
import java.util.Set;
import java.util.regex.Pattern;
import org.databiosphere.workspacedataservice.config.DataImportProperties;
import org.databiosphere.workspacedataservice.generated.ImportRequestServerModel;
import org.databiosphere.workspacedataservice.service.model.exception.ValidationException;
import org.springframework.stereotype.Component;

@Component
public class ImportSourceValidator {
  private final Set<String> allowedSchemes;
  private final Set<Pattern> allowedHosts;

  public ImportSourceValidator(DataImportProperties dataImportProperties) {
    this.allowedSchemes = dataImportProperties.getAllowedSchemes();
    this.allowedHosts = dataImportProperties.getAllowedHosts();
  }

  public void validateImportRequest(ImportRequestServerModel importRequest) {
    URI importUrl = importRequest.getUrl();

    if (!allowedSchemes.contains(importUrl.getScheme())) {
      throw new ValidationException(
          "Files may not be imported from %s URLs.".formatted(importUrl.getScheme()));
    }

    // File URLs don't have a host to validate.
    boolean isFileUrl = importUrl.getScheme().equals("file");
    if (!isFileUrl
        && allowedHosts.stream()
            .noneMatch(allowedHost -> allowedHost.matcher(importUrl.getHost()).matches())) {
      throw new ValidationException(
          "Files may not be imported from %s.".formatted(importUrl.getHost()));
    }
  }
}
