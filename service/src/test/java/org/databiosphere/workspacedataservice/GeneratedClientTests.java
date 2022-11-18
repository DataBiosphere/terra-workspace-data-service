package org.databiosphere.workspacedataservice;

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
import org.databiosphere.workspacedata.model.TsvUploadResponse;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.web.server.LocalServerPort;

import java.io.File;
import java.net.URISyntaxException;
import java.util.List;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;


@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
class GeneratedClientTests {

    private ApiClient apiClient;
    @LocalServerPort
    int port;

    private final UUID instanceId = UUID.randomUUID();
    private final String version = "v0.2";

    @BeforeEach
    void init() throws ApiException {
        apiClient = new ApiClient();
        apiClient.setBasePath("http://localhost:" + port);
        createNewInstance(instanceId);
    }
    @Test
    void uploadTsv() throws ApiException, URISyntaxException {
        RecordsApi recordsApi = new RecordsApi(apiClient);
        TsvUploadResponse tsvUploadResponse = recordsApi.uploadTSV(
                new File(this.getClass().getResource("/small-test.tsv").toURI()),
                instanceId.toString(), version, "foo", null);
        assertThat(tsvUploadResponse.getRecordsModified()).isEqualTo(2);
    }

    @Test
    void uploadTsvWithDifferentColId() throws ApiException, URISyntaxException {
        RecordsApi recordsApi = new RecordsApi(apiClient);
        TsvUploadResponse tsvUploadResponse = recordsApi.uploadTSV(
                new File(this.getClass().getResource("/small-test.tsv").toURI()),
                instanceId.toString(), version, "foo", "greeting");
        assertThat(tsvUploadResponse.getRecordsModified()).isEqualTo(2);
    }

    @Test
    void putAndGetRecords() throws ApiException {
        RecordsApi recordsApi = new RecordsApi(apiClient);
        String recordId = "id1";
        String recordType = "FOO";
        createRecord(recordsApi, recordId, recordType);
        RecordResponse record = recordsApi.getRecord(instanceId.toString(), version, recordType, recordId);
        assertThat(record.getId()).isEqualTo(recordId);
    }

    @Test
    void putAndQuery() throws ApiException {
        RecordsApi recordsApi = new RecordsApi(apiClient);
        String recordType = "type1";
        String recordId = "id1";
        createRecord(recordsApi, recordId, recordType);
        RecordQueryResponse response = recordsApi.queryRecords(new SearchRequest(), instanceId.toString(), version, recordType);
        assertThat(response.getTotalRecords()).isEqualTo(1);
        assertThat(response.getRecords().get(0).getId()).isEqualTo(recordId);
    }

    private void createRecord(RecordsApi recordsApi, String recordId, String recordType) throws ApiException {
        recordsApi.createOrReplaceRecord(new RecordRequest().attributes(new RecordAttributes()),
                instanceId.toString(), version, recordType, recordId);
    }

    @Test
    void describeTypes() throws ApiException {
        String recordType = "FOO";
        createRecord(new RecordsApi(apiClient), "id1", recordType);
        createRecord(new RecordsApi(apiClient), "id1", recordType+"_new");
        SchemaApi schemaApi = new SchemaApi(apiClient);
        RecordTypeSchema schema = schemaApi.describeRecordType(instanceId.toString(), version, recordType);
        assertThat(schema.getName()).isEqualTo(recordType);
        List<RecordTypeSchema> schemas = schemaApi.describeAllRecordTypes(instanceId.toString(), version);
        assertThat(schemas).hasSize(2);
    }

    @Test
    void putPatchAndGetRecord() throws ApiException {
        RecordsApi recordsApi = new RecordsApi(apiClient);
        String recordId = "id1";
        String entityType = "FOO";
        String attributeName = "attr1";
        RecordAttributes recordAttributes = new RecordAttributes();
        recordAttributes.put(attributeName, "Hello");
        recordsApi.createOrReplaceRecord(new RecordRequest().attributes(recordAttributes), instanceId.toString(), version, entityType, recordId);
        recordAttributes.put(attributeName, "Goodbye");
        recordsApi.updateRecord(new RecordRequest().attributes(recordAttributes), instanceId.toString(), version, entityType, recordId);
        RecordResponse record = recordsApi.getRecord(instanceId.toString(), version, entityType, recordId);
        assertThat(record.getAttributes()).containsEntry(attributeName, "Goodbye");
    }

    private void createNewInstance(UUID instanceId) throws ApiException {
        InstancesApi instancesApi = new InstancesApi(apiClient);
        instancesApi.createWDSInstance(instanceId.toString(), version);
    }
}
