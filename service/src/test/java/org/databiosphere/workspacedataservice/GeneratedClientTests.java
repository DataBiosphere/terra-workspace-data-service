package org.databiosphere.workspacedataservice;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.io.File;
import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.UUID;
import java.util.stream.Stream;
import org.apache.commons.io.FileUtils;
import org.databiosphere.workspacedata.api.GeneralWdsInformationApi;
import org.databiosphere.workspacedata.api.InstancesApi;
import org.databiosphere.workspacedata.api.RecordsApi;
import org.databiosphere.workspacedata.api.SchemaApi;
import org.databiosphere.workspacedata.client.ApiClient;
import org.databiosphere.workspacedata.client.ApiException;
import org.databiosphere.workspacedata.model.RecordAttributes;
import org.databiosphere.workspacedata.model.RecordQueryResponse;
import org.databiosphere.workspacedata.model.RecordRequest;
import org.databiosphere.workspacedata.model.RecordResponse;
import org.databiosphere.workspacedata.model.RecordTypeSchema;
import org.databiosphere.workspacedata.model.SearchFilter;
import org.databiosphere.workspacedata.model.SearchRequest;
import org.databiosphere.workspacedata.model.StatusResponse;
import org.databiosphere.workspacedata.model.TsvUploadResponse;
import org.databiosphere.workspacedataservice.common.TestBase;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.lang.Nullable;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.ActiveProfiles;

@ActiveProfiles(profiles = "mock-sam")
@DirtiesContext
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
class GeneratedClientTests extends TestBase {

  private ApiClient apiClient;
  @LocalServerPort int port;

  private final UUID collectionId = UUID.randomUUID();
  private final String version = "v0.2";

  @BeforeEach
  void init() throws ApiException {
    apiClient = new ApiClient();
    apiClient.setBasePath("http://localhost:" + port);
    createNewCollection(collectionId);
  }

  @AfterEach
  void afterEach() throws ApiException {
    deleteCollection(collectionId);
  }

  @Test
  void uploadTsv() throws ApiException, URISyntaxException {
    RecordsApi recordsApi = new RecordsApi(apiClient);
    TsvUploadResponse tsvUploadResponse =
        recordsApi.uploadTSV(
            new File(this.getClass().getResource("/tsv/small-test.tsv").toURI()),
            collectionId.toString(),
            version,
            "foo",
            null);
    assertThat(tsvUploadResponse.getRecordsModified()).isEqualTo(2);
  }

  @Test
  void uploadTsvWithDifferentColId() throws ApiException, URISyntaxException {
    RecordsApi recordsApi = new RecordsApi(apiClient);
    TsvUploadResponse tsvUploadResponse =
        recordsApi.uploadTSV(
            new File(this.getClass().getResource("/tsv/small-no-sys.tsv").toURI()),
            collectionId.toString(),
            version,
            "foo",
            "greeting");
    assertThat(tsvUploadResponse.getRecordsModified()).isEqualTo(2);
  }

  @Test
  void putRecordWithSpecifiedPk() throws ApiException {
    RecordsApi recordsApi = new RecordsApi(apiClient);
    String recordId = "id1";
    String entityType = "FOO";
    String attributeName = "attr1";
    RecordAttributes recordAttributes = new RecordAttributes();
    recordAttributes.put(attributeName, "Hello");
    recordsApi.createOrReplaceRecord(
        new RecordRequest().attributes(recordAttributes),
        collectionId.toString(),
        version,
        entityType,
        recordId,
        "row_id");
    RecordResponse record =
        recordsApi.getRecord(collectionId.toString(), version, entityType, recordId);
    assertThat(record.getAttributes()).containsEntry(attributeName, "Hello");
  }

  @Test
  void putAndGetRecords() throws ApiException {
    RecordsApi recordsApi = new RecordsApi(apiClient);
    String recordId = "id1";
    String recordType = "FOO";
    createRecord(recordsApi, recordId, recordType);
    RecordResponse record =
        recordsApi.getRecord(collectionId.toString(), version, recordType, recordId);
    assertThat(record.getId()).isEqualTo(recordId);
  }

  // verify that TSV downloads work via the client. Unit tests elsewhere assert on TSV correctness;
  // this test asserts that the RecordsApi.getRecordsAsTsv() method succeeds.
  @Test
  void putAndDownloadRecords() throws ApiException, IOException {
    RecordsApi recordsApi = new RecordsApi(apiClient);
    String recordId = "id1";
    String recordType = "FOO";
    createRecord(recordsApi, recordId, recordType);
    File tsvDownload = recordsApi.getRecordsAsTsv(collectionId.toString(), version, recordType);

    List<String> tsvLines = FileUtils.readLines(tsvDownload, StandardCharsets.UTF_8);
    // TSV should have two lines: the header row and one content row
    assertEquals(2, tsvLines.size());
    // check each row; this is trivial since each row has a single column
    assertEquals("sys_name", tsvLines.get(0));
    assertEquals("id1", tsvLines.get(1));
  }

  @Test
  void putAndQuery() throws ApiException {
    RecordsApi recordsApi = new RecordsApi(apiClient);
    String recordType = "type1";
    String recordId = "id1";
    createRecord(recordsApi, recordId, recordType);
    RecordQueryResponse response =
        recordsApi.queryRecords(new SearchRequest(), collectionId.toString(), version, recordType);
    assertThat(response.getTotalRecords()).isEqualTo(1);
    assertThat(response.getRecords().get(0).getId()).isEqualTo(recordId);
  }

  private void createRecord(RecordsApi recordsApi, String recordId, String recordType)
      throws ApiException {
    recordsApi.createOrReplaceRecord(
        new RecordRequest().attributes(new RecordAttributes()),
        collectionId.toString(),
        version,
        recordType,
        recordId,
        null);
  }

  @Test
  void describeTypes() throws ApiException {
    String recordType = "FOO";
    createRecord(new RecordsApi(apiClient), "id1", recordType);
    createRecord(new RecordsApi(apiClient), "id1", recordType + "_new");
    SchemaApi schemaApi = new SchemaApi(apiClient);
    RecordTypeSchema schema =
        schemaApi.describeRecordType(collectionId.toString(), version, recordType);
    assertThat(schema.getName()).isEqualTo(recordType);
    List<RecordTypeSchema> schemas =
        schemaApi.describeAllRecordTypes(collectionId.toString(), version);
    assertThat(schemas).hasSize(2);
    assertThat(schemas.get(0).getPrimaryKey()).isEqualTo("sys_name");
  }

  @Test
  void putPatchAndGetRecord() throws ApiException {
    RecordsApi recordsApi = new RecordsApi(apiClient);
    String recordId = "id1";
    String entityType = "FOO";
    String attributeName = "attr1";
    RecordAttributes recordAttributes = new RecordAttributes();
    recordAttributes.put(attributeName, "Hello");
    recordsApi.createOrReplaceRecord(
        new RecordRequest().attributes(recordAttributes),
        collectionId.toString(),
        version,
        entityType,
        recordId,
        null);
    recordAttributes.put(attributeName, "Goodbye");
    recordsApi.updateRecord(
        new RecordRequest().attributes(recordAttributes),
        collectionId.toString(),
        version,
        entityType,
        recordId);
    RecordResponse record =
        recordsApi.getRecord(collectionId.toString(), version, entityType, recordId);
    assertThat(record.getAttributes()).containsEntry(attributeName, "Goodbye");
  }

  @Test
  void checkStatus() throws ApiException {
    GeneralWdsInformationApi statusApi = new GeneralWdsInformationApi();
    statusApi.setApiClient(apiClient);
    StatusResponse response = statusApi.statusGet();
    assertThat(response.getStatus().equals("UP"));
  }

  // the "small-test.tsv" file has two records, with ids "a" and "b"
  // pairs of (input, expected)
  private static Stream<Arguments> filterIdsProvider() {
    return Stream.of(
        Arguments.of(List.of("a"), List.of("a")),
        Arguments.of(List.of("b"), List.of("b")),
        Arguments.of(List.of("a", "b"), List.of("a", "b")),
        Arguments.of(List.of("b", "c"), List.of("b")),
        Arguments.of(List.of("c", "d"), List.of()),
        Arguments.of(List.of(), List.of()),
        Arguments.of(null, List.of("a", "b")));
  }

  @ParameterizedTest(name = "filter.ids of {0} will result in {1}")
  @MethodSource("filterIdsProvider")
  void filterIds(@Nullable List<String> filterIds, List<String> expected)
      throws ApiException, URISyntaxException {
    RecordsApi recordsApi = new RecordsApi(apiClient);

    String recordType = "foo";

    TsvUploadResponse tsvUploadResponse =
        recordsApi.uploadTSV(
            new File(this.getClass().getResource("/tsv/small-test.tsv").toURI()),
            collectionId.toString(),
            version,
            recordType,
            null);
    assertThat(tsvUploadResponse.getRecordsModified()).isEqualTo(2);

    SearchRequest searchRequest = new SearchRequest();

    if (filterIds != null) {
      SearchFilter searchFilter = new SearchFilter();
      searchFilter.setIds(filterIds);
      searchRequest.setFilter(searchFilter);
    }
    /*
     Note: the Java client will throw an error if searchFilter.getIds() is null, which will happen
     in the following code paths:
       SearchFilter searchFilter = new SearchFilter();
       searchRequest.setFilter(searchFilter);

       ~ or ~

       SearchFilter searchFilter = new SearchFilter();
       searchFilter.setIds(null);
       searchRequest.setFilter(searchFilter);

     This looks to be an openapi-generator issue:
     https://github.com/OpenAPITools/openapi-generator/issues/12549
    */

    RecordQueryResponse recordQueryResponse =
        recordsApi.queryRecords(searchRequest, collectionId.toString(), version, recordType);

    List<String> actual =
        recordQueryResponse.getRecords().stream().map(RecordResponse::getId).toList();

    assertThat(actual).isEqualTo(expected);
  }

  @Test
  void recordAttributesAccess() throws URISyntaxException, ApiException {
    // create the two records "a" and "b" from small-test.tsv
    RecordsApi recordsApi = new RecordsApi(apiClient);

    String recordType = "foo";

    TsvUploadResponse tsvUploadResponse =
        recordsApi.uploadTSV(
            new File(this.getClass().getResource("/tsv/small-test.tsv").toURI()),
            collectionId.toString(),
            version,
            recordType,
            null);
    assertThat(tsvUploadResponse.getRecordsModified()).isEqualTo(2);

    // get record "a"
    RecordResponse recordResponse =
        recordsApi.getRecord(collectionId.toString(), version, recordType, "a");

    // spot-check a couple attributes. These assertions are about validating that the Java client
    // can read RecordAttributes; less about ensuring that the TSV upload was correct.
    RecordAttributes recordAttributes = recordResponse.getAttributes();
    assertThat(recordAttributes.entrySet()).hasSize(14);
    assertThat(recordAttributes).containsEntry("greeting", "hello").containsEntry("double", -2.287);
  }

  private void createNewCollection(UUID collectionId) throws ApiException {
    InstancesApi instancesApi = new InstancesApi(apiClient);
    instancesApi.createWDSInstance(collectionId.toString(), version);
  }

  private void deleteCollection(UUID collection) throws ApiException {
    InstancesApi instancesApi = new InstancesApi(apiClient);
    instancesApi.deleteWDSInstance(collection.toString(), version);
  }
}
