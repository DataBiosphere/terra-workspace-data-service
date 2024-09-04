package org.databiosphere.workspacedataservice.dataimport;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.net.URI;
import java.util.stream.Stream;
import org.databiosphere.workspacedataservice.common.DataPlaneTestBase;
import org.databiosphere.workspacedataservice.config.DataImportProperties;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

@SpringBootTest
class ImportRequirementsFactoryTest extends DataPlaneTestBase {
  @Autowired DataImportProperties dataImportProperties;

  @ParameterizedTest(name = "Imports from {0} should require a private workspace {1}")
  @MethodSource({"requirePrivateWorkspaceTestCases", "bioDataCatalystTestCases"})
  void configuredSourcesRequireAPrivateWorkspace(
      URI importUri, boolean shouldRequirePrivateWorkspace) {
    // Arrange
    ImportRequirementsFactory importRequirementsFactory =
        new ImportRequirementsFactory(dataImportProperties.getSources());

    // Act
    ImportRequirements importRequirements =
        importRequirementsFactory.getRequirementsForImport(importUri);

    // Assert
    assertEquals(shouldRequirePrivateWorkspace, importRequirements.privateWorkspace());
  }

  private static Stream<Arguments> requirePrivateWorkspaceTestCases() {
    return Stream.of(
        Arguments.of(
            /* importUri */ URI.create("https://storage.googleapis.com/test-bucket/file.pfb"),
            /* shouldRequirePrivateWorkspace */ false),
        Arguments.of(
            /* importUri */ URI.create("https://files.terra.bio/file.pfb"),
            /* shouldRequirePrivateWorkspace */ false));
  }

  @ParameterizedTest(name = "Imports from {0} should require protected data policy {1}")
  @MethodSource({"requireProtectedDataPolicyTestCases", "bioDataCatalystTestCases"})
  void configuredSourcesRequireAProtectedDataPolicy(
      URI importUri, boolean shouldRequireProtectedDataPolicy) {
    // Arrange
    ImportRequirementsFactory importRequirementsFactory =
        new ImportRequirementsFactory(dataImportProperties.getSources());

    // Act
    ImportRequirements importRequirements =
        importRequirementsFactory.getRequirementsForImport(importUri);

    // Assert
    assertEquals(shouldRequireProtectedDataPolicy, importRequirements.protectedDataPolicy());
  }

  private static Stream<Arguments> requireProtectedDataPolicyTestCases() {
    return Stream.of(
        Arguments.of(
            /* importUri */ URI.create("https://storage.googleapis.com/test-bucket/file.pfb"),
            /* shouldReuqireProtectedDataPolicy */ false),
        Arguments.of(
            /* importUri */ URI.create("https://files.terra.bio/file.pfb"),
            /* shouldReuqireProtectedDataPolicy */ false));
  }

  /*
   * Imports from BioData Catalyst should require _both_ a private workspace and protected data policy.
   * Thus, these test cases are valid for both configuredSourcesRequireAPrivateWorkspace and
   * configuredSourcesRequireAProtectedDataPolicy.
   */
  private static Stream<Arguments> bioDataCatalystTestCases() {
    return Stream.of(
        Arguments.of(
            /* importUri */ URI.create("https://gen3.biodatacatalyst.nhlbi.nih.gov/file.pfb"),
            /* shouldRequirePrivateWorkspace/ProtectedDataPolicy */ true),
        Arguments.of(
            /* importUri */ URI.create(
                "https://subdomain.gen3.biodatacatalyst.nhlbi.nih.gov/file.pfb"),
            /* shouldRequirePrivateWorkspace/ProtectedDataPolicy */ true),
        Arguments.of(
            /* importUri */ URI.create(
                "https://gen3-biodatacatalyst-nhlbi-nih-gov-pfb-export.s3.amazonaws.com/file.pfb"),
            /* shouldRequirePrivateWorkspace/ProtectedDataPolicy */ true),
        Arguments.of(
            /* importUri */ URI.create(
                "https://s3.amazonaws.com/gen3-biodatacatalyst-nhlbi-nih-gov-pfb-export/file.pfb"),
            /* shouldRequirePrivateWorkspace/ProtectedDataPolicy */ true),
        Arguments.of(
            /* importUri */ URI.create(
                "https://gen3-theanvil-io-pfb-export.s3.amazonaws.com/file.pfb"),
            /* shouldRequirePrivateWorkspace/ProtectedDataPolicy */ true),
        Arguments.of(
            /* importUri */ URI.create(
                "https://s3.amazonaws.com/gen3-theanvil-io-pfb-export/file.pfb"),
            /* shouldRequirePrivateWorkspace/ProtectedDataPolicy */ true));
  }
}
