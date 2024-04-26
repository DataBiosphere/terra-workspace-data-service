package org.databiosphere.workspacedataservice.dataimport;

import java.net.URI;
import java.util.Set;
import java.util.regex.Pattern;
import org.databiosphere.workspacedataservice.config.DataImportProperties;
import org.databiosphere.workspacedataservice.generated.ImportRequestServerModel;
import org.databiosphere.workspacedataservice.service.model.exception.ValidationException;
import org.springframework.stereotype.Component;

@Component
public class ImportValidator {
  private final boolean allowHttpUrls;
  private final Set<Pattern> allowedHosts;

  public ImportValidator(DataImportProperties dataImportProperties) {
    this.allowHttpUrls = dataImportProperties.areHttpUrlsAllowed();
    this.allowedHosts = dataImportProperties.getAllowedHosts();
  }

  public void validateImport(ImportRequestServerModel importRequest) {
    URI importUrl = importRequest.getUrl();
    String urlScheme = importUrl.getScheme();
    if (!(urlScheme.equals("https") || (urlScheme.equals("http") && allowHttpUrls))) {
      throw new ValidationException(
          "Files may not be imported from %s URLs.".formatted(importUrl.getScheme()));
    }

    if (allowedHosts.stream()
        .noneMatch(allowedHost -> allowedHost.matcher(importUrl.getHost()).matches())) {
      throw new ValidationException(
          "Files may not be imported from %s.".formatted(importUrl.getHost()));
    }
  }
}
