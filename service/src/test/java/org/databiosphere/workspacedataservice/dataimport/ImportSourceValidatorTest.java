package org.databiosphere.workspacedataservice.dataimport;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.net.URI;
import org.databiosphere.workspacedataservice.generated.ImportRequestServerModel;
import org.databiosphere.workspacedataservice.service.model.exception.ValidationException;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

@SpringBootTest(properties = {"twds.data-import.allowed-hosts=.*\\.terra\\.bio"})
public class ImportSourceValidatorTest {
  @Autowired ImportSourceValidator importSourceValidator;

  @ParameterizedTest(name = "for import type {0}, should require HTTPS URLs")
  @EnumSource(ImportRequestServerModel.TypeEnum.class)
  void requiresHttpsImportUrls(ImportRequestServerModel.TypeEnum importType) {
    // Arrange
    URI importUri =
        URI.create("http://teststorageaccount.blob.core.windows.net/testcontainer/file");
    ImportRequestServerModel importRequest = new ImportRequestServerModel(importType, importUri);

    // Act/Assert
    ValidationException err =
        assertThrows(
            ValidationException.class,
            () -> importSourceValidator.validateImportRequest(importRequest));
    assertEquals("Files may not be imported from http URLs.", err.getMessage());
  }

  @ParameterizedTest(name = "for import type {0}, should accept files from configured sources")
  @EnumSource(ImportRequestServerModel.TypeEnum.class)
  void allowsImportsFromConfiguredSources(ImportRequestServerModel.TypeEnum importType) {
    // Arrange
    URI importUri = URI.create("https://files.terra.bio/file");
    ImportRequestServerModel importRequest = new ImportRequestServerModel(importType, importUri);

    // Act/Assert
    assertDoesNotThrow(() -> importSourceValidator.validateImportRequest(importRequest));
  }

  @ParameterizedTest(name = "for import type {0}, should reject files from other sources")
  @EnumSource(ImportRequestServerModel.TypeEnum.class)
  void rejectsImportsFromOtherSources(ImportRequestServerModel.TypeEnum importType) {
    // Arrange
    URI importUri = URI.create("https://example.com/file");
    ImportRequestServerModel importRequest = new ImportRequestServerModel(importType, importUri);

    // Act/Assert
    ValidationException err =
        assertThrows(
            ValidationException.class,
            () -> importSourceValidator.validateImportRequest(importRequest));
    assertEquals("Files may not be imported from example.com.", err.getMessage());
  }
}
