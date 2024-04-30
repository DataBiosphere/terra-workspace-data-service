package org.databiosphere.workspacedataservice.dataimport;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.net.URI;
import java.util.stream.Stream;
import org.databiosphere.workspacedataservice.common.TestBase;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

@SpringBootTest
class ImportRequirementsFactoryTest extends TestBase {
  @Autowired ImportRequirementsFactory importRequirementsFactory;

  @ParameterizedTest(name = "Imports from {0} should require protected data policy {1}")
  @MethodSource("requireProtectedDataPolicyTestCases")
  void configuredSourcesRequireAProtectedDataPolicy(
      URI importUri, boolean shouldRequireProtectedDataPolicy) {
    // Act
    ImportRequirements importRequirements =
        importRequirementsFactory.getRequirementsForImport(importUri);

    // Assert
    assertEquals(shouldRequireProtectedDataPolicy, importRequirements.protectedDataPolicy());
  }

  private static Stream<Arguments> requireProtectedDataPolicyTestCases() {
    return Stream.of(
        Arguments.of(URI.create("https://gen3.biodatacatalyst.nhlbi.nih.gov/file.pfb"), true),
        Arguments.of(
            URI.create("https://subdomain.gen3.biodatacatalyst.nhlbi.nih.gov/file.pfb"), true),
        Arguments.of(
            URI.create(
                "https://gen3-biodatacatalyst-nhlbi-nih-gov-pfb-export.s3.amazonaws.com/file.pfb"),
            true),
        Arguments.of(
            URI.create(
                "https://s3.amazonaws.com/gen3-biodatacatalyst-nhlbi-nih-gov-pfb-export/file.pfb"),
            true),
        Arguments.of(
            URI.create("https://gen3-theanvil-io-pfb-export.s3.amazonaws.com/file.pfb"), true),
        Arguments.of(
            URI.create("https://s3.amazonaws.com/gen3-theanvil-io-pfb-export/file.pfb"), true),
        Arguments.of(URI.create("https://storage.googleapis.com/test-bucket/file.pfb"), false),
        Arguments.of(URI.create("https://files.terra.bio/file.pfb"), false));
  }
}
