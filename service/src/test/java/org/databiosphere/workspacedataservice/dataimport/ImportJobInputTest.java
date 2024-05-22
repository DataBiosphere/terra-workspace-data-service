package org.databiosphere.workspacedataservice.dataimport;

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.stream.Stream;
import org.apache.commons.lang3.RandomStringUtils;
import org.databiosphere.workspacedataservice.common.TestBase;
import org.databiosphere.workspacedataservice.dataimport.pfb.PfbImportOptions;
import org.databiosphere.workspacedataservice.dataimport.pfb.PfbJobInput;
import org.databiosphere.workspacedataservice.dataimport.rawlsjson.RawlsJsonImportOptions;
import org.databiosphere.workspacedataservice.dataimport.rawlsjson.RawlsJsonJobInput;
import org.databiosphere.workspacedataservice.dataimport.tdr.TdrManifestImportOptions;
import org.databiosphere.workspacedataservice.dataimport.tdr.TdrManifestJobInput;
import org.databiosphere.workspacedataservice.generated.ImportRequestServerModel;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.MethodSource;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

@SpringBootTest
class ImportJobInputTest extends TestBase {
  @Autowired ObjectMapper objectMapper;

  @ParameterizedTest(name = "with type {0}")
  @EnumSource(ImportRequestServerModel.TypeEnum.class)
  void from(ImportRequestServerModel.TypeEnum type) throws URISyntaxException {
    URI testUri = new URI("https://example.com/?rand=" + RandomStringUtils.randomAlphanumeric(10));
    ImportRequestServerModel importRequest = new ImportRequestServerModel(type, testUri);

    ImportJobInput actual = ImportJobInput.from(importRequest);
    assertEquals(testUri, actual.getUri());
    assertEquals(type, actual.getImportType());
  }

  @ParameterizedTest(name = "serializes {0}")
  @MethodSource("serializeTestCases")
  void serializes(ImportJobInput importJobInput) throws ClassNotFoundException, IOException {
    ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
    ObjectOutputStream objectOutputStream = new ObjectOutputStream(outputStream);
    objectOutputStream.writeObject(importJobInput);
    objectOutputStream.flush();
    objectOutputStream.close();

    ByteArrayInputStream inputStream = new ByteArrayInputStream(outputStream.toByteArray());
    ObjectInputStream objectInputStream = new ObjectInputStream(inputStream);
    ImportJobInput output = (ImportJobInput) objectInputStream.readObject();
    objectInputStream.close();

    assertEquals(importJobInput, output);
  }

  @ParameterizedTest(name = "serializes to JSON {0}")
  @MethodSource("serializeTestCases")
  void serializesToJson(ImportJobInput importJobInput) throws JsonProcessingException {
    String json = objectMapper.writeValueAsString(importJobInput);
    ImportJobInput output = objectMapper.readValue(json, ImportJobInput.class);
    assertEquals(importJobInput, output);
  }

  public static Stream<Arguments> serializeTestCases() {
    return Stream.of(
        Arguments.of(
            new TdrManifestJobInput(
                URI.create("https://data.terra.bio/manifest.json"),
                ImportRequestServerModel.TypeEnum.TDRMANIFEST,
                new TdrManifestImportOptions(true))),
        Arguments.of(
            new TdrManifestJobInput(
                URI.create("https://data.terra.bio/manifest.json"),
                ImportRequestServerModel.TypeEnum.TDRMANIFEST,
                new TdrManifestImportOptions(true))),
        Arguments.of(
            new PfbJobInput(
                URI.create("https://example.com/file.pfb"),
                ImportRequestServerModel.TypeEnum.PFB,
                new PfbImportOptions())),
        Arguments.of(
            new RawlsJsonJobInput(
                URI.create("gs://test-bucket/entities.json"),
                ImportRequestServerModel.TypeEnum.RAWLSJSON,
                new RawlsJsonImportOptions(true))),
        Arguments.of(
            new RawlsJsonJobInput(
                URI.create("gs://test-bucket/entities.json"),
                ImportRequestServerModel.TypeEnum.RAWLSJSON,
                new RawlsJsonImportOptions(false))));
  }
}
