package org.databiosphere.workspacedataservice.dataimport;

import static java.util.Collections.emptySet;

import com.google.common.collect.Sets;
import java.net.URI;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;
import org.databiosphere.workspacedataservice.generated.ImportRequestServerModel;
import org.databiosphere.workspacedataservice.generated.ImportRequestServerModel.TypeEnum;
import org.databiosphere.workspacedataservice.service.model.exception.ValidationException;

public class DefaultImportValidator implements ImportValidator {
  private static final Map<TypeEnum, Set<String>> SUPPORTED_URL_SCHEMES_BY_IMPORT_TYPE =
      Map.of(
          TypeEnum.PFB, Set.of("https"),
          TypeEnum.RAWLSJSON, Set.of("gs"),
          TypeEnum.TDRMANIFEST, Set.of("https"));
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
    TypeEnum importType = importRequest.getType();
    Set<String> schemesSupportedForImportType =
        SUPPORTED_URL_SCHEMES_BY_IMPORT_TYPE.getOrDefault(importType, emptySet());

    URI importUrl = importRequest.getUrl();
    String urlScheme = importUrl.getScheme();
    if (!schemesSupportedForImportType.contains(urlScheme)) {
      throw new ValidationException("Files may not be imported from %s URLs.".formatted(urlScheme));
    }

    // Imports should be allowed from any GCS bucket.
    boolean skipHostValidation = urlScheme.equals("gs");
    if (!skipHostValidation
        && allowedHosts.stream()
            .noneMatch(allowedHost -> allowedHost.matcher(importUrl.getHost()).matches())) {
      throw new ValidationException(
          "Files may not be imported from %s.".formatted(importUrl.getHost()));
    }
  }
}
