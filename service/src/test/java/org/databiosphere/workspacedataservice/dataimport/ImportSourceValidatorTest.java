package org.databiosphere.workspacedataservice.dataimport;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.net.URI;
import org.databiosphere.workspacedataservice.common.TestBase;
import org.databiosphere.workspacedataservice.service.model.exception.ValidationException;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

@SpringBootTest(properties = {"twds.data-import.allowed-hosts=.*\\.terra\\.bio"})
class ImportSourceValidatorTest extends TestBase {
  @Autowired ImportSourceValidator importSourceValidator;

  @Test
  void requiresHttpsImportUrls() {
    // Arrange
    URI importUri =
        URI.create("http://teststorageaccount.blob.core.windows.net/testcontainer/file");

    // Act/Assert
    ValidationException err =
        assertThrows(
            ValidationException.class, () -> importSourceValidator.validateImport(importUri));
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
    assertDoesNotThrow(() -> importSourceValidator.validateImport(importUri));
  }

  @Test
  void allowsImportsFromConfiguredSources() {
    // Arrange
    URI importUri = URI.create("https://files.terra.bio/file");

    // Act/Assert
    assertDoesNotThrow(() -> importSourceValidator.validateImport(importUri));
  }

  @Test
  void rejectsImportsFromOtherSources() {
    // Arrange
    URI importUri = URI.create("https://example.com/file");

    // Act/Assert
    ValidationException err =
        assertThrows(
            ValidationException.class, () -> importSourceValidator.validateImport(importUri));
    assertEquals("Files may not be imported from example.com.", err.getMessage());
  }
}
