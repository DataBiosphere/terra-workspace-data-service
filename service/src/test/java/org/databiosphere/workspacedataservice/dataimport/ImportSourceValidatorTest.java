package org.databiosphere.workspacedataservice.dataimport;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.net.URI;
import org.databiosphere.workspacedataservice.common.TestBase;
import org.databiosphere.workspacedataservice.generated.ImportRequestServerModel.TypeEnum;
import org.databiosphere.workspacedataservice.service.model.exception.ValidationException;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

@SpringBootTest(
    properties = {
      "twds.data-import.allowed-hosts=.*\\.terra\\.bio",
      "twds.data-import.rawls-json-direct-import-bucket=allowed-bucket"
    })
class ImportSourceValidatorTest extends TestBase {
  @Autowired ImportSourceValidatorFactory importSourceValidatorFactory;

  @Test
  void requiresHttpsImportUrls() {
    // Arrange
    URI importUri =
        URI.create("http://teststorageaccount.blob.core.windows.net/testcontainer/file");

    // Act/Assert
    ValidationException err =
        assertThrows(ValidationException.class, () -> defaultValidator().validateImport(importUri));
    assertEquals("Files may not be imported from http URLs.", err.getMessage());
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

    // Act/Assert
    assertDoesNotThrow(() -> defaultValidator().validateImport(importUri));
  }

  @Test
  void allowsImportsFromConfiguredSources() {
    // Arrange
    URI importUri = URI.create("https://files.terra.bio/file");

    // Act/Assert
    assertDoesNotThrow(() -> defaultValidator().validateImport(importUri));
  }

  @Test
  void rejectsImportsFromOtherSources() {
    // Arrange
    URI importUri = URI.create("https://example.com/file");

    // Act/Assert
    ValidationException err =
        assertThrows(ValidationException.class, () -> defaultValidator().validateImport(importUri));
    assertEquals("Files may not be imported from example.com.", err.getMessage());
  }

  @Test
  void rawlsJsonAllowsGcsUrisFromBucketInAllowlist() {
    // Arrange
    URI importUri = URI.create("gs://allowed-bucket/file");

    // Act/Assert
    assertDoesNotThrow(() -> rawlsJsonValidator().validateImport(importUri));
  }

  @Test
  void rawlsJsonDisallowsGcsUrisFromBucketsNotInAllowlist() {
    // Arrange
    URI importUri = URI.create("gs://randobucket/file");

    // Act/Assert
    ValidationException err =
        assertThrows(
            ValidationException.class, () -> rawlsJsonValidator().validateImport(importUri));

    assertEquals("Files may not be imported from randobucket.", err.getMessage());
  }

  @Test
  void rawlsJsonDisallowsNonGcsUris() {
    // Arrange
    URI importUri = URI.create("https://some.random.domain/file");

    // Act/Assert
    ValidationException err =
        assertThrows(
            ValidationException.class, () -> rawlsJsonValidator().validateImport(importUri));

    assertEquals("Files may not be imported from https URLs.", err.getMessage());
  }

  private ImportSourceValidator defaultValidator() {
    return importSourceValidatorFactory.create(TypeEnum.PFB);
  }

  private ImportSourceValidator rawlsJsonValidator() {
    return importSourceValidatorFactory.create(TypeEnum.RAWLSJSON);
  }
}
