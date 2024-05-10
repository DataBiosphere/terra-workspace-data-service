package org.databiosphere.workspacedataservice.dataimport;

import static java.util.Collections.emptyList;
import static org.assertj.core.api.AssertionsForClassTypes.assertThatExceptionOfType;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.when;

import java.net.URI;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.regex.Pattern;
import java.util.stream.Stream;
import org.broadinstitute.dsde.workbench.client.sam.model.RolesAndActions;
import org.broadinstitute.dsde.workbench.client.sam.model.UserResourcesResponse;
import org.databiosphere.workspacedataservice.common.TestBase;
import org.databiosphere.workspacedataservice.config.DataImportProperties.ImportSourceConfig;
import org.databiosphere.workspacedataservice.dataimport.protecteddatasupport.ProtectedDataSupport;
import org.databiosphere.workspacedataservice.generated.ImportRequestServerModel;
import org.databiosphere.workspacedataservice.generated.ImportRequestServerModel.TypeEnum;
import org.databiosphere.workspacedataservice.sam.SamDao;
import org.databiosphere.workspacedataservice.service.model.exception.ValidationException;
import org.databiosphere.workspacedataservice.shared.model.WorkspaceId;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Primary;

@SpringBootTest
class DefaultImportValidatorTest extends TestBase {

  @TestConfiguration
  static class DefaultImportValidatorTestConfiguration {
    @Bean
    @Primary
    DefaultImportValidator getDefaultImportValidatorForTest(
        ProtectedDataSupport protectedDataSupport, SamDao samDao) {
      return new DefaultImportValidator(
          protectedDataSupport,
          samDao,
          /* allowedHttpsHosts */ Set.of(Pattern.compile(".*\\.terra\\.bio")),
          /* sources */ List.of(
              new ImportSourceConfig(
                  /* urls */ List.of(Pattern.compile("protected\\.pfb")),
                  /* requirePrivateWorkspace */ false,
                  /* requireProtectedDataPolicy */ true),
              new ImportSourceConfig(
                  /* urls */ List.of(Pattern.compile("private\\.pfb")),
                  /* requirePrivateWorkspace */ true,
                  /* requireProtectedDataPolicy */ false)),
          /* allowedRawlsBucket */ "test-bucket");
    }
  }

  @MockBean ProtectedDataSupport protectedDataSupport;

  @MockBean SamDao samDao;

  @Autowired DefaultImportValidator importValidator;

  private static final WorkspaceId destinationWorkspaceId = WorkspaceId.of(UUID.randomUUID());

  @Test
  void requiresHttpsImportUrls() {
    // Arrange
    URI importUri =
        URI.create("http://teststorageaccount.blob.core.windows.net/testcontainer/file");
    ImportRequestServerModel importRequest = new ImportRequestServerModel(TypeEnum.PFB, importUri);

    // Act/Assert
    ValidationException err =
        assertThrows(
            ValidationException.class,
            () -> importValidator.validateImport(importRequest, destinationWorkspaceId));
    assertEquals("Files may not be imported from http URLs.", err.getMessage());
  }

  @Test
  void rejectsFileImportUrls() {
    // Arrange
    URI importUri = URI.create("file:///path/to/file");
    ImportRequestServerModel importRequest = new ImportRequestServerModel(TypeEnum.PFB, importUri);

    // Act/Assert
    ValidationException err =
        assertThrows(
            ValidationException.class,
            () -> importValidator.validateImport(importRequest, destinationWorkspaceId));
    assertEquals("Files may not be imported from file URLs.", err.getMessage());
  }

  @ParameterizedTest
  @ValueSource(
      strings = {
        // Azure
        "https://teststorageaccount.blob.core.windows.net/testcontainer/file",
        // GCP
        "https://storage.googleapis.com/testbucket/file",
        // AWS
        "https://s3.amazonaws.com/testbucket/file",
        "https://testbucket.s3.amazonaws.com/file"
      })
  void allowsImportsFromCloudStorage(String cloudStorageUrl) {
    // Arrange
    URI importUri = URI.create(cloudStorageUrl);
    ImportRequestServerModel importRequest = new ImportRequestServerModel(TypeEnum.PFB, importUri);

    // Act/Assert
    assertDoesNotThrow(() -> importValidator.validateImport(importRequest, destinationWorkspaceId));
  }

  @Test
  void allowsImportsFromConfiguredSources() {
    // Arrange
    URI importUri = URI.create("https://files.terra.bio/file");
    ImportRequestServerModel importRequest = new ImportRequestServerModel(TypeEnum.PFB, importUri);

    // Act/Assert
    assertDoesNotThrow(() -> importValidator.validateImport(importRequest, destinationWorkspaceId));
  }

  @Test
  void rejectsImportsFromOtherSources() {
    // Arrange
    URI importUri = URI.create("https://example.com/file");
    ImportRequestServerModel importRequest = new ImportRequestServerModel(TypeEnum.PFB, importUri);

    // Act/Assert
    ValidationException err =
        assertThrows(
            ValidationException.class,
            () -> importValidator.validateImport(importRequest, destinationWorkspaceId));
    assertEquals("Files may not be imported from example.com.", err.getMessage());
  }

  @Test
  void acceptsGsUrlsWithMatchingWorkspaceIdForRawlsJsonImports() {
    // Arrange
    URI importUri =
        URI.create("gs://test-bucket/to-cwds/%s/random.json".formatted(destinationWorkspaceId));
    ImportRequestServerModel importRequest =
        new ImportRequestServerModel(TypeEnum.RAWLSJSON, importUri);

    // Act/Assert
    assertDoesNotThrow(() -> importValidator.validateImport(importRequest, destinationWorkspaceId));
  }

  @Test
  void rejectsGsUrlsWithCorrectBucketAndWorkspaceButUnexpectedFormat() {
    // Arrange
    URI importUri =
        URI.create("gs://test-bucket/random/%s/random.json".formatted(destinationWorkspaceId));
    ImportRequestServerModel importRequest =
        new ImportRequestServerModel(TypeEnum.RAWLSJSON, importUri);

    // Act/Assert
    assertThatExceptionOfType(ValidationException.class)
        .isThrownBy(() -> importValidator.validateImport(importRequest, destinationWorkspaceId))
        .withMessageContaining("/to-cwds/%s/".formatted(destinationWorkspaceId));
  }

  @Test
  void rejectGsUrlsWithMismatchingBucketForJsonImports() {
    // Arrange
    URI importUri =
        URI.create("gs://rando-bucket/to-cwds/%s/random.json".formatted(destinationWorkspaceId));
    ImportRequestServerModel importRequest =
        new ImportRequestServerModel(TypeEnum.RAWLSJSON, importUri);

    // Act/Assert
    assertThatExceptionOfType(ValidationException.class)
        .isThrownBy(
            () -> importValidator.validateImport(importRequest, WorkspaceId.of(UUID.randomUUID())))
        .withMessageContaining("Files may not be imported from rando-bucket.");
  }

  @Test
  void rejectsGsUrlsWithMismatchingWorkspaceId() {
    // Arrange
    UUID otherWorkspaceId = UUID.randomUUID();
    URI importUri =
        URI.create(
            "gs://test-bucket/to-cwds/%s/other-workspace-import.json".formatted(otherWorkspaceId));
    ImportRequestServerModel importRequest =
        new ImportRequestServerModel(TypeEnum.RAWLSJSON, importUri);

    // Act/Assert
    assertThatExceptionOfType(ValidationException.class)
        .isThrownBy(() -> importValidator.validateImport(importRequest, destinationWorkspaceId))
        .withMessageContaining("/to-cwds/%s/".formatted(destinationWorkspaceId));
  }

  @ParameterizedTest
  @MethodSource("requireProtectedWorkspacesForImportsFromConfiguredSourcesTestCases")
  void requireProtectedWorkspacesForImportsFromConfiguredSources(
      URI importUri, boolean workspaceSupportsProtectedDataPolicy, boolean shouldAllowImport) {
    // Arrange
    ImportRequestServerModel importRequest = new ImportRequestServerModel(TypeEnum.PFB, importUri);

    when(protectedDataSupport.workspaceSupportsProtectedDataPolicy(destinationWorkspaceId))
        .thenReturn(workspaceSupportsProtectedDataPolicy);

    // Act
    if (shouldAllowImport) {
      importValidator.validateImport(importRequest, destinationWorkspaceId);
    } else {
      ValidationException err =
          assertThrows(
              ValidationException.class,
              () -> importValidator.validateImport(importRequest, destinationWorkspaceId));
      assertEquals(
          "Data from this source can only be imported into a protected workspace.",
          err.getMessage());
    }
  }

  static Stream<Arguments> requireProtectedWorkspacesForImportsFromConfiguredSourcesTestCases() {
    URI protectedImport = URI.create("https://files.terra.bio/protected.pfb");
    URI unprotectedImport = URI.create("https://files.terra.bio/file.pfb");

    return Stream.of(
        Arguments.of(
            /* importUri */ protectedImport,
            /* workspaceSupportsProtectedDataPolicy */ false,
            /* shouldAllowImport */ false),
        Arguments.of(
            /* importUri */ protectedImport,
            /* workspaceSupportsProtectedDataPolicy */ true,
            /* shouldAllowImport */ true),
        Arguments.of(
            /* importUri */ unprotectedImport,
            /* workspaceSupportsProtectedDataPolicy */ false,
            /* shouldAllowImport */ true),
        Arguments.of(
            /* importUri */ protectedImport,
            /* workspaceSupportsProtectedDataPolicy */ true,
            /* shouldAllowImport */ true));
  }

  @ParameterizedTest
  @MethodSource("requirePrivateWorkspacesForImportsFromConfiguredSourcesTestCases")
  void requirePrivateWorkspacesForImportsFromConfiguredSources(
      URI importUri,
      List<UserResourcesResponse> workspaceResourcesAndPolicies,
      boolean shouldAllowImport) {
    // Arrange
    ImportRequestServerModel importRequest = new ImportRequestServerModel(TypeEnum.PFB, importUri);

    when(samDao.listWorkspaceResourcesAndPolicies()).thenReturn(workspaceResourcesAndPolicies);

    // Act
    if (shouldAllowImport) {
      importValidator.validateImport(importRequest, destinationWorkspaceId);
    } else {
      ValidationException err =
          assertThrows(
              ValidationException.class,
              () -> importValidator.validateImport(importRequest, destinationWorkspaceId));
      assertEquals(
          "Data from this source cannot be imported into a public workspace.", err.getMessage());
    }
  }

  static Stream<Arguments> requirePrivateWorkspacesForImportsFromConfiguredSourcesTestCases() {
    URI privateImport = URI.create("https://files.terra.bio/private.pfb");
    URI otherImport = URI.create("https://files.terra.bio/file.pfb");

    UserResourcesResponse publicWorkspaceResourcesAndPolicies =
        buildUserResourcesResponse(destinationWorkspaceId.id());
    publicWorkspaceResourcesAndPolicies.getPublic().setRoles(List.of("reader"));

    UserResourcesResponse privateWorkspaceResourcesAndPolicies =
        buildUserResourcesResponse(destinationWorkspaceId.id());

    return Stream.of(
        Arguments.of(
            /* importUri */ privateImport,
            /* workspaceResourcesAndPolicies */ List.of(publicWorkspaceResourcesAndPolicies),
            /* shouldAllowImport */ false),
        Arguments.of(
            /* importUri */ privateImport,
            /* workspaceResourcesAndPolicies */ List.of(privateWorkspaceResourcesAndPolicies),
            /* shouldAllowImport */ true),
        Arguments.of(
            /* importUri */ otherImport,
            /* workspaceResourcesAndPolicies */ List.of(publicWorkspaceResourcesAndPolicies),
            /* shouldAllowImport */ true),
        Arguments.of(
            /* importUri */ otherImport,
            /* workspaceResourcesAndPolicies */ List.of(privateWorkspaceResourcesAndPolicies),
            /* shouldAllowImport */ true));
  }

  private static UserResourcesResponse buildUserResourcesResponse(UUID resourceId) {
    UserResourcesResponse userResourcesResponse = new UserResourcesResponse();
    userResourcesResponse.setResourceId(resourceId.toString());

    RolesAndActions directRolesAndActions = new RolesAndActions();
    directRolesAndActions.setActions(emptyList());
    directRolesAndActions.setRoles(List.of("writer"));
    userResourcesResponse.setDirect(directRolesAndActions);

    RolesAndActions publicRolesAndActions = new RolesAndActions();
    publicRolesAndActions.setActions(emptyList());
    publicRolesAndActions.setRoles(emptyList());
    userResourcesResponse.setPublic(publicRolesAndActions);

    return userResourcesResponse;
  }
}
