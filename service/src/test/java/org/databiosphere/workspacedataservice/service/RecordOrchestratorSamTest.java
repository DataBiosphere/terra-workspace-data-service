package org.databiosphere.workspacedataservice.service;

import org.broadinstitute.dsde.workbench.client.sam.ApiException;
import org.broadinstitute.dsde.workbench.client.sam.api.ResourcesApi;
import org.databiosphere.workspacedataservice.dao.RecordDao;
import org.databiosphere.workspacedataservice.sam.SamClientFactory;
import org.databiosphere.workspacedataservice.service.model.RelationCollection;
import org.databiosphere.workspacedataservice.service.model.exception.AuthorizationException;
import org.databiosphere.workspacedataservice.shared.model.*;
import org.databiosphere.workspacedataservice.shared.model.Record;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.mockito.Mockito;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.mock.web.MockMultipartFile;

import java.io.IOException;
import java.io.InputStream;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

import static org.databiosphere.workspacedataservice.service.RecordUtils.VERSION;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.BDDMockito.given;

@SpringBootTest
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class RecordOrchestratorSamTest {

    @Autowired
    private RecordDao recordDao;
    @Autowired private RecordOrchestratorService recordOrchestratorService;
    // mock for the SamClientFactory; since this is a Spring bean we can use @MockBean
    @MockBean
    SamClientFactory mockSamClientFactory;

    // mock for the ResourcesApi class inside the Sam client; since this is not a Spring bean we have to mock it manually
    ResourcesApi mockResourcesApi = Mockito.mock(ResourcesApi.class);


    private static final UUID INSTANCE = UUID.fromString("123e4567-e89b-12d3-a456-426614174000");

    private static final String PRIMARY_KEY = "row_id";
    private static final String RECORD_ID = "aNewRecord";
    private static final String TEST_KEY = "test_key";
    private static final String TEST_VAL = "val";

    @BeforeEach
    void setUp() {
        if (!recordDao.instanceSchemaExists(INSTANCE)) {
            recordDao.createSchema(INSTANCE);
        }
        given(mockSamClientFactory.getResourcesApi())
                .willReturn(mockResourcesApi);

        // clear call history for the mock
        Mockito.clearInvocations(mockResourcesApi);
    }

    @AfterEach
    void cleanUp() {
        recordDao.dropSchema(INSTANCE);
    }

    @Test
    void testUpdateRecordNoPermission() throws ApiException {

        //create record type
        RecordType updateTest = RecordType.valueOf("update_test");
        recordDao.createRecordType(INSTANCE, Collections.emptyMap(), updateTest, new RelationCollection(Collections.emptySet(), Collections.emptySet()), PRIMARY_KEY);

        //create a record to update
        Record rec = new Record(RECORD_ID, updateTest, RecordAttributes.empty());
        recordDao.batchUpsert(INSTANCE, updateTest, Collections.singletonList(rec), Collections.emptyMap());

        // Call to check permissions in Sam does not throw an exception, but returns false -
        // i.e. the current user does not have permission
        given(mockResourcesApi.resourcePermissionV2(anyString(), anyString(), anyString()))
                .willReturn(false);

        RecordRequest req = new RecordRequest(RecordAttributes.empty().putAttribute(TEST_KEY, TEST_VAL));
        assertThrows(AuthorizationException.class,
                () -> recordOrchestratorService.updateSingleRecord(INSTANCE, VERSION, updateTest, RECORD_ID, req),
                "updateSingleRecord should throw if caller does not have write permission in Sam"
        );

        //Record should not have changed
        Record result = recordDao.getSingleRecord(INSTANCE, updateTest, RECORD_ID).get();
        assertNull(result.getAttributes().getAttributeValue(TEST_KEY));
    }

    @Test
    void testUpdateRecordWithPermission() throws ApiException {

        //create record type
        RecordType updateTest = RecordType.valueOf("update_test");
        recordDao.createRecordType(INSTANCE, Collections.emptyMap(), updateTest, new RelationCollection(Collections.emptySet(), Collections.emptySet()), PRIMARY_KEY);

        //create a record to update
        Record rec = new Record(RECORD_ID, updateTest, RecordAttributes.empty());
        recordDao.batchUpsert(INSTANCE, updateTest, Collections.singletonList(rec), Collections.emptyMap());
        // Call to check permissions in Sam returns true - i.e. the current user has permission
        given(mockResourcesApi.resourcePermissionV2(anyString(), anyString(), anyString()))
                .willReturn(true);

        RecordRequest req = new RecordRequest(RecordAttributes.empty().putAttribute(TEST_KEY, TEST_VAL));
        RecordResponse resp = recordOrchestratorService.updateSingleRecord(INSTANCE, VERSION, updateTest, RECORD_ID, req);
        assertEquals(TEST_VAL, resp.recordAttributes().getAttributeValue(TEST_KEY));

        //Record should have changed
        Record result = recordDao.getSingleRecord(INSTANCE, updateTest, RECORD_ID).get();
        assertEquals(TEST_VAL, result.getAttributes().getAttributeValue(TEST_KEY));
    }

    @Test
    void testUpsertNoPermission() throws ApiException {

        //create record type
        RecordType upsertTest = RecordType.valueOf("upsert_test");
        recordDao.createRecordType(INSTANCE, Collections.emptyMap(), upsertTest, new RelationCollection(Collections.emptySet(), Collections.emptySet()), PRIMARY_KEY);

        // Call to check permissions in Sam returns false - i.e. the current user does not have permission
        given(mockResourcesApi.resourcePermissionV2(anyString(), anyString(), anyString()))
                .willReturn(false);

        //attempt to upsert a record
        RecordRequest req = new RecordRequest(RecordAttributes.empty().putAttribute(TEST_KEY, TEST_VAL));
        assertThrows(AuthorizationException.class,
                () -> recordOrchestratorService.upsertSingleRecord(INSTANCE, VERSION, upsertTest, RECORD_ID, Optional.empty(), req),
                "upsertSingleRecord should throw if caller does not have write permission in Sam"
        );

        //Record should not have been upserted
        Optional<Record> result = recordDao.getSingleRecord(INSTANCE, upsertTest, RECORD_ID);
        assert(result.isEmpty());
    }

    @Test
    void testUpsertWithPermission() throws ApiException {
        //create record type
        RecordType upsertTest = RecordType.valueOf("upsert_test");
        recordDao.createRecordType(INSTANCE, Collections.emptyMap(), upsertTest, new RelationCollection(Collections.emptySet(), Collections.emptySet()), PRIMARY_KEY);

        // Call to check permissions in Sam returns true - i.e. the current user does have permission
        given(mockResourcesApi.resourcePermissionV2(anyString(), anyString(), anyString()))
                .willReturn(true);

        //attempt to upsert a record
        RecordRequest req = new RecordRequest(RecordAttributes.empty().putAttribute(TEST_KEY, TEST_VAL));
        ResponseEntity resp = recordOrchestratorService.upsertSingleRecord(INSTANCE, VERSION, upsertTest, RECORD_ID, Optional.empty(), req);
        assertEquals(HttpStatus.CREATED, resp.getStatusCode());

        //Record should exist
        Optional<Record> result = recordDao.getSingleRecord(INSTANCE, upsertTest, RECORD_ID);
        assert(result.isPresent());
        assertEquals(TEST_VAL, result.get().getAttributes().getAttributeValue(TEST_KEY));
    }

    @Test
    void testDeleteRecordNoPermission() throws ApiException {
        //create record type
        RecordType deleteTest = RecordType.valueOf("delete_test");
        recordDao.createRecordType(INSTANCE, Collections.emptyMap(), deleteTest, new RelationCollection(Collections.emptySet(), Collections.emptySet()), PRIMARY_KEY);

        // Call to check permissions in Sam returns false - i.e. the current user does not have permission
        given(mockResourcesApi.resourcePermissionV2(anyString(), anyString(), anyString()))
                .willReturn(false);

        //attempt to delete record
        assertThrows(AuthorizationException.class,
                () -> recordOrchestratorService.deleteSingleRecord(INSTANCE, VERSION, deleteTest, RECORD_ID),
                "deleteSingleRecord should throw if caller does not have write permission in Sam"
        );

        //Record should still exist
        Optional<Record> result = recordDao.getSingleRecord(INSTANCE, deleteTest, RECORD_ID);
        assert(result.isPresent());
    }

    @Test
    void testDeleteRecordWithPermission() throws ApiException {
        //create record type
        RecordType deleteTest = RecordType.valueOf("delete_test");
        recordDao.createRecordType(INSTANCE, Collections.emptyMap(), deleteTest, new RelationCollection(Collections.emptySet(), Collections.emptySet()), PRIMARY_KEY);

        // Call to check permissions in Sam returns true - i.e. the current user does have permission
        given(mockResourcesApi.resourcePermissionV2(anyString(), anyString(), anyString()))
                .willReturn(true);

        //attempt to delete record
        recordOrchestratorService.deleteSingleRecord(INSTANCE, VERSION, deleteTest, RECORD_ID);

        //Record should have been deleted
        Optional<Record> result = recordDao.getSingleRecord(INSTANCE, deleteTest, RECORD_ID);
        assert(result.isEmpty());
    }

    @Test
    void testDeleteRecordTypeNoPermission() throws ApiException {
        //create record type
        RecordType deleteTypeTest = RecordType.valueOf("delete_type_test");
        recordDao.createRecordType(INSTANCE, Collections.emptyMap(), deleteTypeTest, new RelationCollection(Collections.emptySet(), Collections.emptySet()), PRIMARY_KEY);

        // Call to check permissions in Sam returns false - i.e. the current user does not have permission
        given(mockResourcesApi.resourcePermissionV2(anyString(), anyString(), anyString()))
                .willReturn(false);

        //attempt to delete record type
        assertThrows(AuthorizationException.class,
                () -> recordOrchestratorService.deleteRecordType(INSTANCE, VERSION, deleteTypeTest),
                "deleteRecordType should throw if caller does not have write permission in Sam"
        );

        //Record type should still exist
        assert(recordDao.recordTypeExists(INSTANCE, deleteTypeTest));
    }

    @Test
    void testDeleteRecordTypeWithPermission() throws ApiException {
        //create record type
        RecordType deleteTypeTest = RecordType.valueOf("delete_type_test");
        recordDao.createRecordType(INSTANCE, Collections.emptyMap(), deleteTypeTest, new RelationCollection(Collections.emptySet(), Collections.emptySet()), PRIMARY_KEY);

        // Call to check permissions in Sam returns true - i.e. the current user does have permission
        given(mockResourcesApi.resourcePermissionV2(anyString(), anyString(), anyString()))
                .willReturn(true);

        //attempt to delete record type
        recordOrchestratorService.deleteRecordType(INSTANCE, VERSION, deleteTypeTest);

        //Record type should have been deleted
        assertFalse(recordDao.recordTypeExists(INSTANCE, deleteTypeTest));
    }

    @Test
    void testTsvUploadWithPermission() throws ApiException, IOException {
        // Call to check permissions in Sam returns true - i.e. the current user does have permission
        given(mockResourcesApi.resourcePermissionV2(anyString(), anyString(), anyString()))
                .willReturn(true);

        //attempt to upload tsv
        MockMultipartFile file = new MockMultipartFile("records", "simple.tsv", MediaType.TEXT_PLAIN_VALUE,
                ("col_1\tcol_2\t" + PRIMARY_KEY + "\n" + "Fido\tJerry\t" + RECORD_ID + "\n").getBytes());
        RecordType tsvTest = RecordType.valueOf("tsv_test");
        recordOrchestratorService.tsvUpload(INSTANCE, VERSION, tsvTest, Optional.of(PRIMARY_KEY), file);

        //Tsv should have been uploaded
        assert(recordDao.recordTypeExists(INSTANCE, tsvTest));
        List<Record> result = recordDao.queryForRecords(tsvTest, 1, 0, "asc", null, INSTANCE);
        assertEquals(1, result.size());
        assertEquals(RECORD_ID, result.get(0).getId());
    }

    @Test
    void testTsvUploadNoPermission() throws ApiException, IOException {
        // Call to check permissions in Sam returns false - i.e. the current user does not have permission
        given(mockResourcesApi.resourcePermissionV2(anyString(), anyString(), anyString()))
                .willReturn(false);

        //attempt to upload tsv
        MockMultipartFile file = new MockMultipartFile("records", "simple.tsv", MediaType.TEXT_PLAIN_VALUE,
                ("col_1\tcol_2\t" + PRIMARY_KEY + "\n" + "Fido\tJerry\t" + RECORD_ID + "\n").getBytes());
        RecordType tsvTest = RecordType.valueOf("tsv_test");
        assertThrows(AuthorizationException.class,
                () -> recordOrchestratorService.tsvUpload(INSTANCE, VERSION, tsvTest, Optional.of(PRIMARY_KEY), file),
                "tsvUpload should throw if caller does not have write permission in Sam"
        );

        //Tsv should not have been uploaded
        assertFalse(recordDao.recordTypeExists(INSTANCE, tsvTest));
    }

    @Test
    void testStreamingWriteWithPermission() throws ApiException {
        // Call to check permissions in Sam returns true - i.e. the current user does have permission
        given(mockResourcesApi.resourcePermissionV2(anyString(), anyString(), anyString()))
                .willReturn(true);

        //attempt to batch upload
        InputStream is = RecordOrchestratorSamTest.class.getResourceAsStream("/batch_write_tsv_data.json");
        RecordType streamTest = RecordType.valueOf("stream_test");
        recordOrchestratorService.streamingWrite(INSTANCE, VERSION, streamTest, Optional.empty(), is);

        //Records should have been uploaded
        assert(recordDao.recordTypeExists(INSTANCE, streamTest));
        List<Record> result = recordDao.queryForRecords(streamTest, 5, 0, "asc", null, INSTANCE);
        assertEquals(2, result.size());
    }

    @Test
    void testStreamingWriteNoPermission() throws ApiException {
        // Call to check permissions in Sam returns false - i.e. the current user does not have permission
        given(mockResourcesApi.resourcePermissionV2(anyString(), anyString(), anyString()))
                .willReturn(false);

        //attempt to batch upload
        InputStream is = RecordOrchestratorSamTest.class.getResourceAsStream("/batch_write_tsv_data.json");
        RecordType streamTest = RecordType.valueOf("stream_test");
        assertThrows(AuthorizationException.class,
                () -> recordOrchestratorService.streamingWrite(INSTANCE, VERSION, streamTest, Optional.empty(), is),
                "streamingWrite should throw if caller does not have write permission in Sam"
        );

        //Records should not have been uploaded
        assertFalse(recordDao.recordTypeExists(INSTANCE, streamTest));
    }

}
