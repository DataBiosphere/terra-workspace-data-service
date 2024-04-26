package org.databiosphere.workspacedataservice.dataimport;

import static java.util.Collections.emptySet;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.net.URI;
import java.util.Set;
import java.util.regex.Pattern;
import org.databiosphere.workspacedataservice.common.TestBase;
import org.databiosphere.workspacedataservice.generated.ImportRequestServerModel;
import org.databiosphere.workspacedataservice.generated.ImportRequestServerModel.TypeEnum;
import org.databiosphere.workspacedataservice.service.model.exception.ValidationException;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

class DefaultImportValidatorTest extends TestBase {
  @Test
  void requiresHttpsImportUrls() {
    // Arrange
    ImportValidator importValidator = new DefaultImportValidator(emptySet());

    URI importUri =
        URI.create("http://teststorageaccount.blob.core.windows.net/testcontainer/file");
    ImportRequestServerModel importRequest = new ImportRequestServerModel(TypeEnum.PFB, importUri);

    // Act/Assert
    ValidationException err =
        assertThrows(
            ValidationException.class, () -> importValidator.validateImport(importRequest));
    assertEquals("Files may not be imported from http URLs.", err.getMessage());
  }

  @Test
  void rejectsFileImportUrls() {
    // Arrange
    ImportValidator importValidator = new DefaultImportValidator(emptySet());

    URI importUri = URI.create("file:///path/to/file");
    ImportRequestServerModel importRequest = new ImportRequestServerModel(TypeEnum.PFB, importUri);

    // Act/Assert
    ValidationException err =
        assertThrows(
            ValidationException.class, () -> importValidator.validateImport(importRequest));
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
    ImportValidator importValidator = new DefaultImportValidator(emptySet());

    URI importUri = URI.create(cloudStorageUrl);
    ImportRequestServerModel importRequest = new ImportRequestServerModel(TypeEnum.PFB, importUri);

    // Act/Assert
    assertDoesNotThrow(() -> importValidator.validateImport(importRequest));
  }

  @Test
  void allowsImportsFromConfiguredSources() {
    // Arrange
    ImportValidator importValidator =
        new DefaultImportValidator(Set.of(Pattern.compile(".*\\.terra\\.bio")));

    URI importUri = URI.create("https://files.terra.bio/file");
    ImportRequestServerModel importRequest = new ImportRequestServerModel(TypeEnum.PFB, importUri);

    // Act/Assert
    assertDoesNotThrow(() -> importValidator.validateImport(importRequest));
  }

  @Test
  void rejectsImportsFromOtherSources() {
    // Arrange
    ImportValidator importValidator =
        new DefaultImportValidator(Set.of(Pattern.compile(".*\\.terra\\.bio")));

    URI importUri = URI.create("https://example.com/file");
    ImportRequestServerModel importRequest = new ImportRequestServerModel(TypeEnum.PFB, importUri);

    // Act/Assert
    ValidationException err =
        assertThrows(
            ValidationException.class, () -> importValidator.validateImport(importRequest));
    assertEquals("Files may not be imported from example.com.", err.getMessage());
  }
}
