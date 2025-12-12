package org.databiosphere.workspacedataservice.dataimport;

import static java.util.Collections.emptySet;
import static java.util.regex.Pattern.compile;

import com.google.common.collect.ImmutableMap;
import io.micrometer.common.util.StringUtils;
import java.net.URI;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import org.broadinstitute.dsde.workbench.client.sam.model.UserResourcesResponse;
import org.databiosphere.workspacedataservice.config.DataImportProperties.ImportSourceConfig;
import org.databiosphere.workspacedataservice.config.DrsImportProperties;
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
  private static final String SCHEME_DRS = "drs";
  private static final Map<TypeEnum, Set<String>> SUPPORTED_URL_SCHEMES_BY_IMPORT_TYPE =
      Map.of(
          TypeEnum.PFB, Set.of(SCHEME_HTTPS, SCHEME_DRS),
          TypeEnum.RAWLSJSON, Set.of(SCHEME_GS),
          TypeEnum.TDRMANIFEST, Set.of(SCHEME_HTTPS));
  private final Map<String, Set<Pattern>> allowedHostsByScheme;
  private final ImportRequirementsFactory importRequirementsFactory;
  private final ProtectedDataSupport protectedDataSupport;
  private final SamDao samDao;
  private final ConnectivityChecker connectivityChecker;
  private final Set<Pattern> sourceUrlPatterns;

  public DefaultImportValidator(
      ProtectedDataSupport protectedDataSupport,
      SamDao samDao,
      Set<Pattern> allowedHttpsHosts,
      List<ImportSourceConfig> sources,
      @Nullable String allowedRawlsBucket,
      ConnectivityChecker connectivityChecker,
      DrsImportProperties drsImportProperties) {

    var allowedHostsBuilder =
        ImmutableMap.<String, Set<Pattern>>builder()
            .put(SCHEME_HTTPS, allowedHttpsHosts)
            .put(
                SCHEME_DRS,
                drsImportProperties.getAllowedHosts().stream()
                    .map(Pattern::compile)
                    .collect(Collectors.toSet()));

    if (StringUtils.isNotBlank(allowedRawlsBucket)) {
      allowedHostsBuilder.put(SCHEME_GS, Set.of(compile(allowedRawlsBucket)));
    } else {
      allowedHostsBuilder.put(SCHEME_GS, emptySet());
    }

    this.allowedHostsByScheme = allowedHostsBuilder.build();
    this.sourceUrlPatterns =
        sources == null
            ? emptySet()
            : sources.stream()
                .flatMap(source -> source.urls().stream())
                .collect(Collectors.toSet());
    this.importRequirementsFactory = new ImportRequirementsFactory(sources);
    this.protectedDataSupport = protectedDataSupport;
    this.samDao = samDao;
    this.connectivityChecker = connectivityChecker;
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

    boolean isHostAllowed =
        getAllowedHosts(importRequest).stream()
            .anyMatch(allowedHost -> allowedHost.matcher(importUrl.getHost()).matches());

    boolean isUrlAllowed =
        sourceUrlPatterns.stream()
            .anyMatch(urlPattern -> urlPattern.matcher(importUrl.toString()).find());

    // If URL is not specifically allow-listed in either allowed-hosts or
    // protection required list, then it's not allowed.
    if (!isHostAllowed && !isUrlAllowed) {
      throw new ValidationException(
          "Files may not be imported from %s.".formatted(importUrl.getHost()));
    }

    if (importType == TypeEnum.RAWLSJSON) {
      validatePathBelongsToWorkspace(importRequest.getUrl().getPath(), destinationWorkspaceId);
    }
    validateDestinationWorkspace(importRequest, destinationWorkspaceId);

    try {
      connectivityChecker.validateConnectivity(importUrl);
    } catch (Exception e) {
      throw new ValidationException("Unable to connect to import URI: " + e.getMessage());
    }
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

    // Apply auth domain groups if present and always applied, else defer
    if (requirements.alwaysApplyAuthDomains()
        && !requirements.requiredAuthDomainGroups().isEmpty()) {
      protectedDataSupport.addAuthDomainGroupsToWorkspace(
          destinationWorkspaceId, requirements.requiredAuthDomainGroups());
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
}
