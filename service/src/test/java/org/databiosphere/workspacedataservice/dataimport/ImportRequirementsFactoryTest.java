package org.databiosphere.workspacedataservice.dataimport;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.net.URI;
import java.util.stream.Stream;
import org.databiosphere.workspacedataservice.common.TestBase;
import org.databiosphere.workspacedataservice.config.DataImportProperties;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

@SpringBootTest
class ImportRequirementsFactoryTest extends TestBase {
  @Autowired DataImportProperties dataImportProperties;

  @ParameterizedTest(name = "Imports from {0} should require a private workspace {1}")
  @MethodSource("requirePrivateWorkspaceTestCases")
  void configuredSourcesRequireAPrivateWorkspace(
      URI importUri, boolean shouldRequireProtectedDataPolicy) {
    // Arrange
    ImportRequirementsFactory importRequirementsFactory =
        new ImportRequirementsFactory(dataImportProperties.getSources());

    // Act
    ImportRequirements importRequirements =
        importRequirementsFactory.getRequirementsForImport(importUri);

    // Assert
    assertEquals(shouldRequireProtectedDataPolicy, importRequirements.privateWorkspace());
  }

  private static Stream<Arguments> requirePrivateWorkspaceTestCases() {
    return Stream.concat(
        mockBioDataCatalystUrls().map(bdcUrl -> Arguments.of(bdcUrl, true)),
        Stream.of(
            Arguments.of(URI.create("https://storage.googleapis.com/test-bucket/file.pfb"), false),
            Arguments.of(URI.create("https://files.terra.bio/file.pfb"), false)));
  }

  @ParameterizedTest(name = "Imports from {0} should require protected data policy {1}")
  @MethodSource("requireProtectedDataPolicyTestCases")
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
    return Stream.concat(
        mockBioDataCatalystUrls().map(bdcUrl -> Arguments.of(bdcUrl, true)),
        Stream.of(
            Arguments.of(URI.create("https://storage.googleapis.com/test-bucket/file.pfb"), false),
            Arguments.of(URI.create("https://files.terra.bio/file.pfb"), false)));
  }

  private static Stream<URI> mockBioDataCatalystUrls() {
    return Stream.of(
        URI.create("https://gen3.biodatacatalyst.nhlbi.nih.gov/file.pfb"),
        URI.create("https://subdomain.gen3.biodatacatalyst.nhlbi.nih.gov/file.pfb"),
        URI.create(
            "https://gen3-biodatacatalyst-nhlbi-nih-gov-pfb-export.s3.amazonaws.com/file.pfb"),
        URI.create(
            "https://s3.amazonaws.com/gen3-biodatacatalyst-nhlbi-nih-gov-pfb-export/file.pfb"),
        URI.create("https://gen3-theanvil-io-pfb-export.s3.amazonaws.com/file.pfb"),
        URI.create("https://s3.amazonaws.com/gen3-theanvil-io-pfb-export/file.pfb"));
  }
}
