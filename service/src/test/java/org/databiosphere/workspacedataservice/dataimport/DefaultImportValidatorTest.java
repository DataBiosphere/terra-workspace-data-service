package org.databiosphere.workspacedataservice.dataimport;

import static org.assertj.core.api.AssertionsForClassTypes.assertThatExceptionOfType;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.when;

import bio.terra.workspace.model.WorkspaceDescription;
import bio.terra.workspace.model.WsmPolicyInput;
import java.net.URI;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.regex.Pattern;
import java.util.stream.Stream;
import org.databiosphere.workspacedataservice.common.TestBase;
import org.databiosphere.workspacedataservice.config.DataImportProperties.ImportSourceConfig;
import org.databiosphere.workspacedataservice.generated.ImportRequestServerModel;
import org.databiosphere.workspacedataservice.generated.ImportRequestServerModel.TypeEnum;
import org.databiosphere.workspacedataservice.service.model.exception.ValidationException;
import org.databiosphere.workspacedataservice.shared.model.WorkspaceId;
import org.databiosphere.workspacedataservice.workspacemanager.WorkspaceManagerDao;
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
    DefaultImportValidator getDefaultImportValidatorForTest(WorkspaceManagerDao wsmDao) {
      return new DefaultImportValidator(
          wsmDao,
          /* allowedHttpsHosts */ Set.of(Pattern.compile(".*\\.terra\\.bio")),
          /* sources */ List.of(
              new ImportSourceConfig(List.of(Pattern.compile("protected\\.pfb")), true)),
          /* allowedRawlsBucket */ "test-bucket");
    }
  }

  @MockBean WorkspaceManagerDao wsmDao;

  @Autowired DefaultImportValidator importValidator;

  private final WorkspaceId destinationWorkspaceId = WorkspaceId.of(UUID.randomUUID());

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
      URI importUri, WorkspaceDescription workspaceDescription, boolean shouldAllowImport) {
    // Arrange
    ImportRequestServerModel importRequest = new ImportRequestServerModel(TypeEnum.PFB, importUri);

    when(wsmDao.getWorkspace(destinationWorkspaceId.id())).thenReturn(workspaceDescription);

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

    WorkspaceDescription workspaceWithoutProtectedDataPolicy = new WorkspaceDescription();

    WorkspaceDescription workspaceWithProtectedDataPolicy = new WorkspaceDescription();
    WsmPolicyInput policy = new WsmPolicyInput();
    policy.setNamespace("terra");
    policy.setName("protected-data");
    workspaceWithProtectedDataPolicy.setPolicies(List.of(policy));

    return Stream.of(
        Arguments.of(protectedImport, workspaceWithoutProtectedDataPolicy, false),
        Arguments.of(protectedImport, workspaceWithProtectedDataPolicy, true),
        Arguments.of(unprotectedImport, workspaceWithoutProtectedDataPolicy, true),
        Arguments.of(protectedImport, workspaceWithProtectedDataPolicy, true));
  }
}
