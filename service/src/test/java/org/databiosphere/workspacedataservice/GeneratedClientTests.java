package org.databiosphere.workspacedataservice;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.File;
import java.net.URISyntaxException;
import java.util.List;
import java.util.UUID;
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
import org.databiosphere.workspacedata.model.SearchRequest;
import org.databiosphere.workspacedata.model.StatusResponse;
import org.databiosphere.workspacedata.model.TsvUploadResponse;
import org.databiosphere.workspacedataservice.common.TestBase;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.server.LocalServerPort;
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

  private void createNewCollection(UUID collectionId) throws ApiException {
    InstancesApi instancesApi = new InstancesApi(apiClient);
    instancesApi.createWDSInstance(collectionId.toString(), version);
  }

  private void deleteCollection(UUID collection) throws ApiException {
    InstancesApi instancesApi = new InstancesApi(apiClient);
    instancesApi.deleteWDSInstance(collection.toString(), version);
  }
}
