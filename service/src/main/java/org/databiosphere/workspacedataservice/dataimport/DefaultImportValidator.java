package org.databiosphere.workspacedataservice.dataimport;

import com.google.common.collect.Sets;
import java.net.URI;
import java.util.Set;
import java.util.regex.Pattern;
import org.databiosphere.workspacedataservice.generated.ImportRequestServerModel;
import org.databiosphere.workspacedataservice.service.model.exception.ValidationException;

public class DefaultImportValidator implements ImportValidator {
  private static final Set<Pattern> ALWAYS_ALLOWED_HOSTS =
      Set.of(
          Pattern.compile("storage\\.googleapis\\.com"),
          Pattern.compile(".*\\.core\\.windows\\.net"),
          // S3 allows multiple URL formats
          // https://docs.aws.amazon.com/AmazonS3/latest/userguide/VirtualHosting.html
          Pattern.compile("s3\\.amazonaws\\.com"), // path style legacy global endpoint
          Pattern.compile(".*\\.s3\\.amazonaws\\.com") // virtual host style legacy global endpoint
          );
  private final Set<Pattern> allowedHosts;

  public DefaultImportValidator(Set<Pattern> allowedHosts) {
    this.allowedHosts = Sets.union(ALWAYS_ALLOWED_HOSTS, allowedHosts);
  }

  public void validateImport(ImportRequestServerModel importRequest) {
    URI importUrl = importRequest.getUrl();
    String urlScheme = importUrl.getScheme();
    if (!urlScheme.equals("https")) {
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
