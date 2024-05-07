package org.databiosphere.workspacedataservice.dataimport;

import static java.util.Collections.emptyList;
import static java.util.Collections.emptySet;
import static java.util.regex.Pattern.compile;

import bio.terra.workspace.model.WorkspaceDescription;
import bio.terra.workspace.model.WsmPolicyInput;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Sets;
import io.micrometer.common.util.StringUtils;
import java.net.URI;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.databiosphere.workspacedataservice.config.DataImportProperties.ImportSourceConfig;
import org.databiosphere.workspacedataservice.generated.ImportRequestServerModel;
import org.databiosphere.workspacedataservice.generated.ImportRequestServerModel.TypeEnum;
import org.databiosphere.workspacedataservice.policy.PolicyUtils;
import org.databiosphere.workspacedataservice.service.model.exception.ValidationException;
import org.databiosphere.workspacedataservice.shared.model.WorkspaceId;
import org.databiosphere.workspacedataservice.workspacemanager.WorkspaceManagerDao;
import org.databiosphere.workspacedataservice.workspacemanager.WorkspaceManagerException;
import org.springframework.http.HttpStatus;
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
  private final WorkspaceManagerDao wsmDao;

  public DefaultImportValidator(
      WorkspaceManagerDao wsmDao,
      Set<Pattern> allowedHttpsHosts,
      List<ImportSourceConfig> sources,
      @Nullable String allowedRawlsBucket) {
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
    this.wsmDao = wsmDao;
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
        && !checkWorkspaceHasProtectedDataPolicy(destinationWorkspaceId)) {
      throw new ValidationException(
          "Data from this source can only be imported into a protected workspace.");
    }
  }

  private boolean checkWorkspaceHasProtectedDataPolicy(WorkspaceId workspaceId) {
    try {
      WorkspaceDescription workspace = wsmDao.getWorkspace(workspaceId.id());
      List<WsmPolicyInput> workspacePolicies =
          Optional.ofNullable(workspace.getPolicies()).orElse(emptyList());
      return workspacePolicies.stream().anyMatch(PolicyUtils::isProtectedDataPolicy);
    } catch (WorkspaceManagerException e) {
      if (e.getStatusCode().equals(HttpStatus.NOT_FOUND)) {
        return false;
      } else {
        throw e;
      }
    }
  }
}
