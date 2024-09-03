package org.databiosphere.workspacedataservice.controller;

import static org.assertj.core.api.Assertions.assertThat;
import static org.databiosphere.workspacedataservice.service.model.DataTypeMapping.*;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.MappingIterator;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectReader;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;
import java.util.stream.Stream;
import org.apache.commons.lang3.RandomStringUtils;
import org.databiosphere.workspacedataservice.TestUtils;
import org.databiosphere.workspacedataservice.common.DataPlaneTestBase;
import org.databiosphere.workspacedataservice.config.TwdsProperties;
import org.databiosphere.workspacedataservice.dao.RecordDao;
import org.databiosphere.workspacedataservice.generated.CollectionRequestServerModel;
import org.databiosphere.workspacedataservice.generated.CollectionServerModel;
import org.databiosphere.workspacedataservice.service.CollectionService;
import org.databiosphere.workspacedataservice.service.model.DataTypeMapping;
import org.databiosphere.workspacedataservice.shared.model.BatchResponse;
import org.databiosphere.workspacedataservice.shared.model.RecordAttributes;
import org.databiosphere.workspacedataservice.shared.model.RecordRequest;
import org.databiosphere.workspacedataservice.shared.model.RecordResponse;
import org.databiosphere.workspacedataservice.shared.model.RecordType;
import org.databiosphere.workspacedataservice.shared.model.TsvUploadResponse;
import org.databiosphere.workspacedataservice.shared.model.attributes.JsonAttribute;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.core.io.Resource;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.mock.web.MockMultipartFile;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.ActiveProfiles;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@ActiveProfiles(profiles = "mock-sam")
@DirtiesContext
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
class TsvDownloadTest extends DataPlaneTestBase {

  @Autowired private TestRestTemplate restTemplate;

  @Autowired private RecordController recordController;
  @Autowired private CollectionController collectionController;
  @Autowired private RecordDao recordDao;
  @Autowired private ObjectMapper mapper;
  private String version;
  private UUID collectionId;

  @Autowired private ObjectReader tsvReader;

  @Autowired TwdsProperties twdsProperties;

  @Autowired CollectionService collectionService;
  @Autowired NamedParameterJdbcTemplate namedTemplate;

  @BeforeEach
  void init() {
    version = "v0.2";
    CollectionRequestServerModel collectionRequestServerModel = new CollectionRequestServerModel();
    collectionRequestServerModel.setName(RandomStringUtils.randomAlphabetic(16));
    collectionRequestServerModel.setDescription("description");
    ResponseEntity<CollectionServerModel> createdCollection =
        collectionController.createCollectionV1(
            twdsProperties.workspaceId().id(), collectionRequestServerModel);
    collectionId = Objects.requireNonNull(createdCollection.getBody()).getId();
  }

  @AfterEach
  void tearDown() {
    TestUtils.cleanAllCollections(collectionService, namedTemplate);
  }

  @ParameterizedTest(name = "PK name {0} should be honored")
  @ValueSource(
      strings = {
        "Alfalfa",
        "buckWheat",
        "boo-yah",
        "sample id",
        "sample_id",
        "buttHead",
        "may 12 sample"
      })
  void tsvUploadWithChosenPrimaryKeyFollowedByDownload(String primaryKeyName) throws IOException {
    MockMultipartFile file =
        new MockMultipartFile(
            "records",
            "simple.tsv",
            MediaType.TEXT_PLAIN_VALUE,
            ("col_1\tcol_2\t" + primaryKeyName + "\n" + "Fido\tJerry\t" + primaryKeyName + "_val\n")
                .getBytes());
    String recordType = primaryKeyName + "_rt";
    recordController.tsvUpload(
        collectionId, version, RecordType.valueOf(recordType), Optional.of(primaryKeyName), file);
    HttpHeaders headers = new HttpHeaders();
    ResponseEntity<Resource> stream =
        restTemplate.exchange(
            "/{instanceId}/tsv/{version}/{recordType}",
            HttpMethod.GET,
            new HttpEntity<>(headers),
            Resource.class,
            collectionId,
            version,
            recordType);
    InputStream inputStream = Objects.requireNonNull(stream.getBody()).getInputStream();
    InputStreamReader reader = new InputStreamReader(inputStream, StandardCharsets.UTF_8);
    MappingIterator<RecordAttributes> tsvIterator = tsvReader.readValues(reader);
    RecordAttributes recordAttributes = tsvIterator.next();
    assertThat(recordAttributes.getAttributeValue(primaryKeyName))
        .isEqualTo(primaryKeyName + "_val");
    assertThat(recordAttributes.getAttributeValue("col_1")).isEqualTo("Fido");
    assertThat(recordAttributes.getAttributeValue("col_2")).isEqualTo("Jerry");
    assertThat(tsvIterator.hasNext()).isFalse();
    reader.close();
  }

  @Test
  void batchWriteFollowedByTsvDownload() throws IOException {
    RecordType recordType = RecordType.valueOf("bar");

    InputStream is = TsvDownloadTest.class.getResourceAsStream("/batch-write/write-tsv-data.json");
    ResponseEntity<BatchResponse> response =
        recordController.streamingWrite(collectionId, version, recordType, Optional.empty(), is);
    assertThat(response.getStatusCode()).isEqualTo(HttpStatus.OK);
    assertThat(Objects.requireNonNull(response.getBody()).recordsModified()).isEqualTo(2);
    HttpHeaders headers = new HttpHeaders();
    ResponseEntity<Resource> stream =
        restTemplate.exchange(
            "/{instanceId}/tsv/{version}/{recordType}",
            HttpMethod.GET,
            new HttpEntity<>(headers),
            Resource.class,
            collectionId,
            version,
            recordType);
    InputStream inputStream = Objects.requireNonNull(stream.getBody()).getInputStream();
    InputStreamReader reader = new InputStreamReader(inputStream, StandardCharsets.UTF_8);
    MappingIterator<RecordAttributes> tsvIterator = tsvReader.readValues(reader);
    RecordAttributes recordAttributes = tsvIterator.next();
    assertThat(recordAttributes.getAttributeValue("description")).isEqualTo("Embedded\tTab");
    recordAttributes = tsvIterator.next();
    assertThat(recordAttributes.getAttributeValue("description")).isEqualTo("\n,Weird\n String");
    assertThat(recordAttributes.getAttributeValue("location")).isEqualTo("Cambridge, \"MA\"");
    assertThat(recordAttributes.getAttributeValue("unicodeData")).isEqualTo("\uD83D\uDCA9ȇ");
    assertThat(tsvIterator.hasNext()).isFalse();
    reader.close();
  }

  private Stream<Arguments> tsvExemplarData() throws JsonProcessingException {
    /* Arguments are sets:
       - first value is the Object to insert as an attribute
       - second value is the expected data type that the object creates
       - third value is the text that would be contained in a TSV cell when downloading the attribute
    */
    return Stream.of(
        Arguments.of(Boolean.TRUE, BOOLEAN, "true"),
        Arguments.of(Boolean.FALSE, BOOLEAN, "false"),
        Arguments.of("hello", STRING, "hello"),
        Arguments.of("embedded\ttab", STRING, "\"embedded\ttab\""),
        Arguments.of("2021-10-03", DATE, "2021-10-03"),
        Arguments.of("2021-10-03T19:01:23", DATE_TIME, "2021-10-03T19:01:23"),
        Arguments.of(BigDecimal.valueOf(789), NUMBER, "789"),
        Arguments.of(BigDecimal.valueOf(25.5), NUMBER, "25.5"),
        Arguments.of(
            "https://accountname.blob.core.windows.net/container-1/blob1",
            FILE,
            "https://accountname.blob.core.windows.net/container-1/blob1"),
        Arguments.of("terra-wds:/target/1", RELATION, "terra-wds:/target/1"),
        Arguments.of(
            "{\"foo\": \"bar\", \"arr\": [2,4,6]}",
            JSON,
            "\"{\"\"arr\"\":[2,4,6],\"\"foo\"\":\"\"bar\"\"}\""),
        Arguments.of(List.of(), ARRAY_OF_STRING, "[]"),
        Arguments.of(
            List.of("foo", "bar", "baz"),
            ARRAY_OF_STRING,
            "\"[\"\"foo\"\",\"\"bar\"\",\"\"baz\"\"]\""),
        Arguments.of(
            List.of(BigDecimal.valueOf(1), BigDecimal.valueOf(3), BigDecimal.valueOf(5)),
            ARRAY_OF_NUMBER,
            "[1,3,5]"),
        Arguments.of(
            List.of(Boolean.TRUE, Boolean.FALSE, Boolean.TRUE),
            ARRAY_OF_BOOLEAN,
            "[true,false,true]"),
        Arguments.of(
            List.of(LocalDate.parse("2021-10-03"), LocalDate.parse("2022-11-04")),
            ARRAY_OF_DATE,
            "\"[\"\"2021-10-03\"\",\"\"2022-11-04\"\"]\""),
        Arguments.of(
            List.of(
                LocalDateTime.parse("2021-10-03T19:01:23"),
                LocalDateTime.parse("2021-11-04T20:02:24")),
            ARRAY_OF_DATE_TIME,
            "\"[\"\"2021-10-03T19:01:23\"\",\"\"2021-11-04T20:02:24\"\"]\""),
        Arguments.of(
            List.of(
                "drs://drs.example.org/file_id_1",
                "https://accountname.blob.core.windows.net/container-2/blob2"),
            ARRAY_OF_FILE,
            "\"[\"\"drs://drs.example.org/file_id_1\"\",\"\"https://accountname.blob.core.windows.net/container-2/blob2\"\"]\""),
        Arguments.of(
            List.of("terra-wds:/target/1", "terra-wds:/target/1"),
            ARRAY_OF_RELATION,
            "\"[\"\"terra-wds:/target/1\"\",\"\"terra-wds:/target/1\"\"]\""),
        // array of json packets
        Arguments.of(
            List.of(
                new JsonAttribute(mapper.readTree("{\"value\":\"foo\"}")),
                new JsonAttribute(mapper.readTree("{\"value\":\"bar\"}")),
                new JsonAttribute(mapper.readTree("{\"value\":\"baz\"}"))),
            ARRAY_OF_JSON,
            "\"[{\"\"value\"\":\"\"foo\"\"},{\"\"value\"\":\"\"bar\"\"},{\"\"value\"\":\"\"baz\"\"}]\""),
        // nested arrays
        Arguments.of(
            List.of(
                new JsonAttribute(mapper.readTree("[1]")),
                new JsonAttribute(mapper.readTree("[2,3]")),
                new JsonAttribute(mapper.readTree("[4,5,6]"))),
            ARRAY_OF_JSON,
            "[[1],[2,3],[4,5,6]]"),
        Arguments.of(
            List.of(
                new JsonAttribute(mapper.readTree("[\"one\"]")),
                new JsonAttribute(mapper.readTree("[\"two\",\"three\"]")),
                new JsonAttribute(mapper.readTree("[\"four\",\"five\",\"six\"]"))),
            ARRAY_OF_JSON,
            "\"[[\"\"one\"\"],[\"\"two\"\",\"\"three\"\"],[\"\"four\"\",\"\"five\"\",\"\"six\"\"]]\""),
        // array of mixed json types
        Arguments.of(
            List.of(
                new JsonAttribute(mapper.readTree("[1,2,3]")),
                new JsonAttribute(mapper.readTree("[\"four\",\"five\"]")),
                new JsonAttribute(mapper.readTree("67")),
                new JsonAttribute(mapper.readTree("{\"some\":\"object\",\"with\":[\"nesting\"]}"))),
            ARRAY_OF_JSON,
            "\"[[1,2,3],[\"\"four\"\",\"\"five\"\"],67,{\"\"some\"\":\"\"object\"\",\"\"with\"\":[\"\"nesting\"\"]}]\""));
  }

  // upload a given input value using the JSON APIs, and validate WDS created the expected datatype.
  // download that column as a TSV and validate the download matches the expected value.
  @ParameterizedTest(name = "{1} with val {2}")
  @MethodSource("tsvExemplarData")
  void tsvDownloadDataTypeFidelity(
      Object toUpload, DataTypeMapping expectedDataType, String expectedCellValue)
      throws IOException {
    RecordType recordType = RecordType.valueOf("myType");
    String recordId = "123";
    String attrName = "attrName";

    // upload a record to serve as the target for any relations
    RecordRequest targetRequest = new RecordRequest(new RecordAttributes(Map.of()));
    ResponseEntity<RecordResponse> targetResponse =
        recordController.upsertSingleRecord(
            collectionId,
            version,
            RecordType.valueOf("target"),
            "1",
            Optional.empty(),
            targetRequest);
    assertThat(targetResponse.getStatusCode()).isEqualTo(HttpStatus.CREATED);

    // upload a record containing the attribute in question
    RecordAttributes uploadAttrs = new RecordAttributes(Map.of(attrName, toUpload));
    RecordRequest recordRequest = new RecordRequest(uploadAttrs);
    ResponseEntity<RecordResponse> response =
        recordController.upsertSingleRecord(
            collectionId, version, recordType, recordId, Optional.empty(), recordRequest);
    assertThat(response.getStatusCode()).isEqualTo(HttpStatus.CREATED);

    // verify the backend datatype
    Map<String, DataTypeMapping> tableSchema =
        recordDao.getExistingTableSchema(collectionId, recordType);
    assertThat(tableSchema.keySet()).contains(attrName);
    DataTypeMapping actualDataType = tableSchema.get(attrName);
    assertThat(actualDataType).isEqualTo(expectedDataType);

    // get the TSV
    HttpHeaders headers = new HttpHeaders();
    ResponseEntity<Resource> stream =
        restTemplate.exchange(
            "/{instanceId}/tsv/{version}/{recordType}",
            HttpMethod.GET,
            new HttpEntity<>(headers),
            Resource.class,
            collectionId,
            version,
            recordType);

    // read the TSV contents. Note that this test intentionally reads the TSV as strings - we don't
    // use
    // a MappingIterator<RecordAttributes>, for example. This allows us to assert on the string
    // representation
    // of values within the TSV.
    try (InputStream inputStream = Objects.requireNonNull(stream.getBody()).getInputStream();
        BufferedReader reader =
            new BufferedReader(new InputStreamReader(inputStream, StandardCharsets.UTF_8))) {
      String firstLine = reader.readLine();
      assertThat(firstLine).isEqualTo("sys_name\t" + attrName);

      String secondLine = reader.readLine();
      assertThat(secondLine).isEqualTo(recordId + "\t" + expectedCellValue);
    }
  }

  // TODO: this tsv upload test shouldn't be in a class named "TsvDownloadTest"
  // upload a given input value using the JSON APIs, and validate WDS created the expected datatype.
  // then, upload a given TSV cell into the same column and validate the datatype did not change.
  @ParameterizedTest(name = "{1} with val {2}")
  @MethodSource("tsvExemplarData")
  void tsvUploadDataTypeFidelity(
      Object toUpload, DataTypeMapping expectedDataType, String cellValue) throws IOException {
    RecordType recordType = RecordType.valueOf("myType");
    String recordId = "123";
    String attrName = "attrName";

    // upload a record to serve as the target for any relations
    RecordRequest targetRequest = new RecordRequest(new RecordAttributes(Map.of()));
    ResponseEntity<RecordResponse> targetResponse =
        recordController.upsertSingleRecord(
            collectionId,
            version,
            RecordType.valueOf("target"),
            "1",
            Optional.empty(),
            targetRequest);
    assertThat(targetResponse.getStatusCode()).isEqualTo(HttpStatus.CREATED);

    // upload a record containing the attribute in question
    RecordAttributes uploadAttrs = new RecordAttributes(Map.of(attrName, toUpload));
    RecordRequest recordRequest = new RecordRequest(uploadAttrs);
    ResponseEntity<RecordResponse> response =
        recordController.upsertSingleRecord(
            collectionId, version, recordType, recordId, Optional.empty(), recordRequest);
    assertThat(response.getStatusCode()).isEqualTo(HttpStatus.CREATED);

    // verify the backend datatype
    Map<String, DataTypeMapping> tableSchema =
        recordDao.getExistingTableSchema(collectionId, recordType);
    assertThat(tableSchema.keySet()).contains(attrName);
    DataTypeMapping actualDataType = tableSchema.get(attrName);
    assertThat(actualDataType).isEqualTo(expectedDataType);

    // create a TSV
    String tsvContents = "sys_name\t" + attrName + "\n" + recordId + "\t" + cellValue + "\n";

    // upload the TSV
    MockMultipartFile file =
        new MockMultipartFile(
            "records", "roundTrip.tsv", MediaType.TEXT_PLAIN_VALUE, tsvContents.getBytes());
    ResponseEntity<TsvUploadResponse> uploadResponse =
        recordController.tsvUpload(collectionId, version, recordType, Optional.empty(), file);
    assertThat(uploadResponse.getStatusCode()).isEqualTo(HttpStatus.OK);

    // verify the backend datatype again; it should not have changed
    tableSchema = recordDao.getExistingTableSchema(collectionId, recordType);
    assertThat(tableSchema.keySet()).contains(attrName);
    actualDataType = tableSchema.get(attrName);
    assertThat(actualDataType).isEqualTo(expectedDataType);
  }
}
