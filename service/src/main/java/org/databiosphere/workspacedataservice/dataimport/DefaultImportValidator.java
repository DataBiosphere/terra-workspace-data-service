package org.databiosphere.workspacedataservice.dataimport;

import static java.util.Collections.emptySet;
import static java.util.regex.Pattern.compile;

import com.google.api.client.http.HttpStatusCodes;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Sets;
import io.micrometer.common.util.StringUtils;
import java.net.HttpURLConnection;
import java.net.URI;
import java.net.URL;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.broadinstitute.dsde.workbench.client.sam.model.UserResourcesResponse;
import org.databiosphere.workspacedataservice.config.DataImportProperties.ImportSourceConfig;
import org.databiosphere.workspacedataservice.dataimport.protecteddatasupport.ProtectedDataSupport;
import org.databiosphere.workspacedataservice.generated.ImportRequestServerModel;
import org.databiosphere.workspacedataservice.generated.ImportRequestServerModel.TypeEnum;
import org.databiosphere.workspacedataservice.sam.SamDao;
import org.databiosphere.workspacedataservice.service.model.exception.ValidationException;
import org.databiosphere.workspacedataservice.shared.model.WorkspaceId;
import org.springframework.lang.Nullable;

public class DefaultImportValidator implements ImportValidator {
  private static final String SCHEME_HTTPS = "https";
  private static final String SCHEME_GS = "gs";
  private static final Map<TypeEnum, Set<String>> SUPPORTED_URL_SCHEMES_BY_IMPORT_TYPE =
      Map.of(
          TypeEnum.PFB, Set.of(SCHEME_HTTPS),
          TypeEnum.RAWLSJSON, Set.of(SCHEME_GS),
          TypeEnum.TDRMANIFEST, Set.of(SCHEME_HTTPS));
  private static final Set<Pattern> ALWAYS_ALLOWED_HOSTS =
      Set.of(
          compile("storage\\.googleapis\\.com"),
          compile(".*\\.core\\.windows\\.net"),
          // S3 allows multiple URL formats
          // https://docs.aws.amazon.com/AmazonS3/latest/userguide/VirtualHosting.html
          compile("s3\\.amazonaws\\.com"), // path style legacy global endpoint
          compile(".*\\.s3\\.amazonaws\\.com") // virtual host style legacy global endpoint
          );
  private final Map<String, Set<Pattern>> allowedHostsByScheme;
  private final ImportRequirementsFactory importRequirementsFactory;
  private final ProtectedDataSupport protectedDataSupport;
  private final SamDao samDao;
  private final Boolean connectivityCheckEnabled;

  public DefaultImportValidator(
      ProtectedDataSupport protectedDataSupport,
      SamDao samDao,
      Set<Pattern> allowedHttpsHosts,
      List<ImportSourceConfig> sources,
      @Nullable String allowedRawlsBucket,
      Boolean connectivityCheckEnabled) {
    var allowedHostsBuilder =
        ImmutableMap.<String, Set<Pattern>>builder()
            .put(SCHEME_HTTPS, Sets.union(ALWAYS_ALLOWED_HOSTS, allowedHttpsHosts));

    if (StringUtils.isNotBlank(allowedRawlsBucket)) {
      allowedHostsBuilder.put(SCHEME_GS, Set.of(compile(allowedRawlsBucket)));
    } else {
      allowedHostsBuilder.put(SCHEME_GS, emptySet());
    }

    this.allowedHostsByScheme = allowedHostsBuilder.build();
    this.importRequirementsFactory = new ImportRequirementsFactory(sources);
    this.protectedDataSupport = protectedDataSupport;
    this.samDao = samDao;
    this.connectivityCheckEnabled = connectivityCheckEnabled;
  }

  private Set<Pattern> getAllowedHosts(ImportRequestServerModel importRequest) {
    return allowedHostsByScheme.get(importRequest.getUrl().getScheme());
  }

  public void validateImport(
      ImportRequestServerModel importRequest, WorkspaceId destinationWorkspaceId) {
    TypeEnum importType = importRequest.getType();
    Set<String> schemesSupportedForImportType =
        SUPPORTED_URL_SCHEMES_BY_IMPORT_TYPE.getOrDefault(importType, emptySet());

    URI importUrl = importRequest.getUrl();

    if (!importUrl.isAbsolute()) {
      throw new ValidationException("Invalid import URL.");
    }

    String urlScheme = importUrl.getScheme();
    if (!schemesSupportedForImportType.contains(urlScheme)) {
      throw new ValidationException("Files may not be imported from %s URLs.".formatted(urlScheme));
    }

    if (getAllowedHosts(importRequest).stream()
        .noneMatch(allowedHost -> allowedHost.matcher(importUrl.getHost()).matches())) {
      throw new ValidationException(
          "Files may not be imported from %s.".formatted(importUrl.getHost()));
    }

    if (importType == TypeEnum.RAWLSJSON) {
      validatePathBelongsToWorkspace(importRequest.getUrl().getPath(), destinationWorkspaceId);
    }
    validateDestinationWorkspace(importRequest, destinationWorkspaceId);

    validateConnectivity(importUrl);
  }

  private static final String URI_TEMPLATE = "^/to-cwds/%s/.*\\.json$";

  private void validatePathBelongsToWorkspace(String path, WorkspaceId workspaceId) {
    Pattern expectedPattern = compile(URI_TEMPLATE.formatted(workspaceId.toString()));
    Matcher matcher = expectedPattern.matcher(path);

    if (!matcher.matches()) {
      throw new ValidationException(
          "Expected URI with format %s".formatted(expectedPattern.toString()));
    }
  }

  private void validateDestinationWorkspace(
      ImportRequestServerModel importRequest, WorkspaceId destinationWorkspaceId) {
    ImportRequirements requirements =
        importRequirementsFactory.getRequirementsForImport(importRequest.getUrl());

    if (requirements.protectedDataPolicy()
        && !protectedDataSupport.workspaceSupportsProtectedDataPolicy(destinationWorkspaceId)) {
      throw new ValidationException(
          "Data from this source can only be imported into a protected workspace.");
    }

    if (requirements.privateWorkspace() && !checkWorkspaceIsPrivate(destinationWorkspaceId)) {
      throw new ValidationException(
          "Data from this source cannot be imported into a public workspace.");
    }
  }

  private boolean checkWorkspaceIsPrivate(WorkspaceId workspaceId) {
    // It's inefficient to request _all_ workspaces here when we're only interested in
    // one of them, but Sam does not provide a way to retrieve this information for
    // only a specific resource.
    List<UserResourcesResponse> resources = samDao.listWorkspaceResourcesAndPolicies();

    // Before import validation, we've verified that the user can write to the destination
    // workspace. So it's safe to assume the workspace is in this list.
    UserResourcesResponse workspaceResource =
        resources.stream()
            .filter(resource -> resource.getResourceId().equals(workspaceId.toString()))
            .findFirst()
            .orElseThrow();
    return workspaceResource.getPublic().getRoles().isEmpty()
        && workspaceResource.getPublic().getActions().isEmpty();
  }

  private boolean validateConnectivity(URI importUrl) {
    // short-circuit if not enabled
    if (!connectivityCheckEnabled) {
      return true;
    }

    // we only validate connectivity for https:// urls;
    // we may want to validate gs:// urls at some future point
    if (!Objects.equals(importUrl.getScheme(), SCHEME_HTTPS)) {
      return true;
    }

    int code;
    try {
      URL urlObject = importUrl.toURL();
      HttpURLConnection conn = (HttpURLConnection) urlObject.openConnection();
      conn.setRequestMethod("HEAD");
      conn.setConnectTimeout(5000);
      conn.setReadTimeout(5000);

      code = conn.getResponseCode();
    } catch (Exception e) {
      throw new ValidationException("Can't connect to the import URI: " + e.getMessage());
    }

    if (!HttpStatusCodes.isSuccess(code)) {
      throw new ValidationException("URI to be imported returned status code " + code);
    }

    return true;
  }
}
