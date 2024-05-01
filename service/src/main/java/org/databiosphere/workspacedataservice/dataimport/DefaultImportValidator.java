package org.databiosphere.workspacedataservice.dataimport;

import static java.util.Collections.emptySet;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Sets;
import io.micrometer.common.util.StringUtils;
import java.net.URI;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;
import org.databiosphere.workspacedataservice.generated.ImportRequestServerModel;
import org.databiosphere.workspacedataservice.generated.ImportRequestServerModel.TypeEnum;
import org.databiosphere.workspacedataservice.service.model.exception.ValidationException;
import org.springframework.lang.Nullable;

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
  private final Map<String, Set<Pattern>> allowedHostsByScheme;

  public DefaultImportValidator(
      Set<Pattern> allowedHttpsHosts, @Nullable String allowedRawlsBucket) {
    var allowedHostsBuilder =
        ImmutableMap.<String, Set<Pattern>>builder()
            .put("https", Sets.union(ALWAYS_ALLOWED_HOSTS, allowedHttpsHosts));

    if (StringUtils.isNotBlank(allowedRawlsBucket)) {
      allowedHostsBuilder.put("gs", Set.of(Pattern.compile(allowedRawlsBucket)));
    } else {
      allowedHostsBuilder.put("gs", emptySet());
    }

    this.allowedHostsByScheme = allowedHostsBuilder.build();
  }

  private Set<Pattern> getAllowedHosts(ImportRequestServerModel importRequest) {
    return allowedHostsByScheme.get(importRequest.getUrl().getScheme());
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

    if (getAllowedHosts(importRequest).stream()
        .noneMatch(allowedHost -> allowedHost.matcher(importUrl.getHost()).matches())) {
      throw new ValidationException(
          "Files may not be imported from %s.".formatted(importUrl.getHost()));
    }
  }
}
