package org.databiosphere.workspacedataservice.dataimport;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.stream.Stream;
import org.apache.commons.lang3.RandomStringUtils;
import org.databiosphere.workspacedataservice.common.JsonUtils;
import org.databiosphere.workspacedataservice.common.TestBase;
import org.databiosphere.workspacedataservice.dataimport.pfb.PfbImportOptions;
import org.databiosphere.workspacedataservice.dataimport.rawlsjson.RawlsJsonImportOptions;
import org.databiosphere.workspacedataservice.dataimport.tdr.TdrManifestImportOptions;
import org.databiosphere.workspacedataservice.generated.ImportRequestServerModel;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.MethodSource;
import org.springframework.boot.test.context.SpringBootTest;

@SpringBootTest
class ImportJobInputTest extends TestBase {

  @ParameterizedTest(name = "with type {0}")
  @EnumSource(ImportRequestServerModel.TypeEnum.class)
  void from(ImportRequestServerModel.TypeEnum type) throws URISyntaxException {
    URI testUri = new URI("https://example.com/?rand=" + RandomStringUtils.randomAlphanumeric(10));
    ImportRequestServerModel importRequest = new ImportRequestServerModel(type, testUri);

    ImportJobInput actual = ImportJobInput.from(importRequest);
    assertEquals(testUri, actual.uri());
    assertEquals(type, actual.importType());
  }

  @ParameterizedTest(name = "serializes {0}")
  @MethodSource("serializeTestCases")
  void serializes(ImportJobInput importJobInput) {
    ImportJobInput output =
        JsonUtils.parse(JsonUtils.stringify(importJobInput), ImportJobInput.class);
    assertEquals(importJobInput, output);
  }

  public static Stream<Arguments> serializeTestCases() {
    return Stream.of(
        Arguments.of(
            new ImportJobInput(
                URI.create("https://data.terra.bio/manifest.json"),
                ImportRequestServerModel.TypeEnum.TDRMANIFEST,
                new TdrManifestImportOptions(true))),
        Arguments.of(
            new ImportJobInput(
                URI.create("https://data.terra.bio/manifest.json"),
                ImportRequestServerModel.TypeEnum.TDRMANIFEST,
                new TdrManifestImportOptions(true))),
        Arguments.of(
            new ImportJobInput(
                URI.create("https://example.com/file.pfb"),
                ImportRequestServerModel.TypeEnum.PFB,
                new PfbImportOptions())),
        Arguments.of(
            new ImportJobInput(
                URI.create("gs://test-bucket/entities.json"),
                ImportRequestServerModel.TypeEnum.RAWLSJSON,
                new RawlsJsonImportOptions(true))),
        Arguments.of(
            new ImportJobInput(
                URI.create("gs://test-bucket/entities.json"),
                ImportRequestServerModel.TypeEnum.RAWLSJSON,
                new RawlsJsonImportOptions(false))));
  }
}
