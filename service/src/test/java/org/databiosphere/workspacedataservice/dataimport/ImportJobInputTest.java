package org.databiosphere.workspacedataservice.dataimport;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.net.URI;
import java.net.URISyntaxException;
import org.apache.commons.lang3.RandomStringUtils;
import org.databiosphere.workspacedataservice.generated.ImportRequestServerModel;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

class ImportJobInputTest {

  @ParameterizedTest(name = "with type {0}")
  @EnumSource(ImportRequestServerModel.TypeEnum.class)
  void from(ImportRequestServerModel.TypeEnum type) throws URISyntaxException {
    URI testUri = new URI("https://example.com/?rand=" + RandomStringUtils.randomAlphanumeric(10));
    ImportRequestServerModel importRequest = new ImportRequestServerModel(type, testUri);

    ImportJobInput actual = ImportJobInput.from(importRequest);
    assertEquals(testUri, actual.getUri());
    assertEquals(type, actual.getImportType());
  }
}
