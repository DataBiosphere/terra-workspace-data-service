package org.databiosphere.workspacedataservice.dataimport;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.when;

import java.net.URI;
import java.util.List;
import java.util.regex.Pattern;
import java.util.stream.Stream;
import org.databiosphere.workspacedataservice.common.ControlPlaneTestBase;
import org.databiosphere.workspacedataservice.config.DataImportProperties;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.bean.override.mockito.MockitoBean;

@SpringBootTest
class ImportRequirementsFactoryTest extends ControlPlaneTestBase {
  @MockitoBean DataImportProperties dataImportProperties;

  @BeforeEach
  void setup() {
    // Mock the sources
    List<DataImportProperties.ImportSourceConfig> mockSources =
        List.of(
            new DataImportProperties.ImportSourceConfig(
                List.of(
                    Pattern.compile("^https:\\/\\/.*gen3\\.biodatacatalyst\\.nhlbi\\.nih\\.gov\\/"),
                    Pattern.compile(
                        "^https:\\/\\/gen3-biodatacatalyst-nhlbi-nih-gov-pfb-export\\.s3\\.amazonaws\\.com\\/"),
                    Pattern.compile(
                        "^https:\\/\\/s3\\.amazonaws\\.com\\/gen3-biodatacatalyst-nhlbi-nih-gov-pfb-export\\/"),
                    Pattern.compile(
                        "^https:\\/\\/pic-sure-auth-prod-data-export\\.s3\\.amazonaws\\.com\\/"),
                    Pattern.compile(
                        "^https:\\/\\/s3\\.amazonaws\\.com\\/pic-sure-auth-prod-data-export\\/"),
                    Pattern.compile(
                        "^https:\\/\\/s3\\.amazonaws\\.com\\/edu-ucsc-gi-platform-anvil-prod-storage-anvilprod\\.us-east-1\\/"),
                    Pattern.compile("^https:\\/\\/nih-nhlbi-.*\\.amazonaws\\.com\\/"),
                    Pattern.compile(
                        "^https:\\/\\/gen3-theanvil-io-pfb-export\\.s3\\.amazonaws\\.com\\/"),
                    Pattern.compile(
                        "^https:\\/\\/s3\\.amazonaws\\.com\\/gen3-theanvil-io-pfb-export\\/")),
                true, // requirePrivateWorkspace
                true, // requireProtectedDataPolicy
                List.of("mock-auth-group")));
    when(dataImportProperties.getSources()).thenReturn(mockSources);
  }

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
            /* shouldRequireProtectedDataPolicy */ false),
        Arguments.of(
            /* importUri */ URI.create("https://files.terra.bio/file.pfb"),
            /* shouldRequireProtectedDataPolicy */ false));
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
            /* shouldRequirePrivateWorkspace/ProtectedDataPolicy */ true),
        Arguments.of(
            /* importUri */ URI.create(
                "https://nih-nhlbi-topmed-released-phs000810-abc.amazonaws.com/parent-EFG_-phs000810-hij2.avro"),
            /* shouldRequirePrivateWorkspace/ProtectedDataPolicy */ true));
  }
}
