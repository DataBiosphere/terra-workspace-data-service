package org.databiosphere.workspacedataservice.service;

import org.databiosphere.workspacedataservice.dao.InstanceDao;
import org.databiosphere.workspacedataservice.shared.model.RecordAttributes;
import org.databiosphere.workspacedataservice.shared.model.RecordQueryResponse;
import org.databiosphere.workspacedataservice.shared.model.RecordRequest;
import org.databiosphere.workspacedataservice.shared.model.RecordResponse;
import org.databiosphere.workspacedataservice.shared.model.RecordType;
import org.databiosphere.workspacedataservice.shared.model.SearchColumn;
import org.databiosphere.workspacedataservice.shared.model.SearchRequest;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.http.ResponseEntity;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.ActiveProfiles;

import java.util.List;
import java.util.Optional;
import java.util.UUID;

import static org.databiosphere.workspacedataservice.service.RecordUtils.VERSION;
import static org.junit.jupiter.api.Assertions.assertEquals;

@ActiveProfiles(profiles = { "mock-sam" })
@DirtiesContext
@SpringBootTest
class SearchTest {

    @Autowired private InstanceDao instanceDao;
    @Autowired private RecordOrchestratorService recordOrchestratorService;

    private static final UUID INSTANCE = UUID.fromString("123e4567-e89b-12d3-a456-426614174000");
    private static final RecordType TEST_TYPE = RecordType.valueOf("test");

    private static final String TEST_KEY = "test_key";

    @BeforeEach
    void setUp() {
        if (!instanceDao.instanceSchemaExists(INSTANCE)) {
            instanceDao.createSchema(INSTANCE);
        }
    }

    @AfterEach
    void cleanUp() {
        instanceDao.dropSchema(INSTANCE);
    }

    @Test
    void findInColumn() {
        // set up exemplar records
        String firstRecord = "r1";
        String firstVal = "v1";
        String secondRecord = "r2";
        String secondVal = "v2";
        String thirdRecord = "r3";
        String thirdVal = "v3";

        testCreateRecord(firstRecord, TEST_KEY, firstVal);
        testCreateRecord(secondRecord, TEST_KEY, secondVal);
        testCreateRecord(thirdRecord, TEST_KEY, thirdVal);

        // perform single-column exact-match search
        SearchRequest searchRequest = new SearchRequest();
        searchRequest.setSearchColumnList(List.of(new SearchColumn(TEST_KEY, secondVal, null)));

        RecordQueryResponse resp = recordOrchestratorService.queryForRecords(
            INSTANCE,
            TEST_TYPE,
            VERSION,
            searchRequest
        );

        // verify found records
        // TODO: fix the totalRecords value!
        // assertEquals(1, resp.totalRecords());
        assertEquals(1, resp.records().size());
        testContainsRecord(secondRecord, TEST_KEY, secondVal, resp.records());
    }


    private void testCreateRecord(String newRecordId, String testKey, String testVal) {
        testCreateRecord(newRecordId, testKey, testVal, TEST_TYPE);
    }

    private void testCreateRecord(String newRecordId, String testKey, String testVal, RecordType newRecordType) {
        RecordRequest recordRequest =  new RecordRequest(RecordAttributes.empty().putAttribute(testKey, testVal));

        ResponseEntity<RecordResponse> response = recordOrchestratorService.upsertSingleRecord(
            INSTANCE,
            VERSION,
            newRecordType,
            newRecordId,
            Optional.empty(),
            recordRequest
        );

        assertEquals(newRecordId, response.getBody().recordId());
    }

    private void testGetRecord(String newRecordId, String testKey, String testVal) {
        RecordResponse recordResponse = recordOrchestratorService.getSingleRecord(
            INSTANCE, VERSION, TEST_TYPE, newRecordId
        );
        assertEquals(testVal, recordResponse.recordAttributes().getAttributeValue(testKey));
    }

    private void testContainsRecord(String recordId, String testKey, String testVal, List<RecordResponse> respList) {
        boolean found = respList.stream()
            .anyMatch(recordResponse ->
                          recordResponse.recordId().equals(recordId) &&
                              recordResponse.recordAttributes().getAttributeValue(testKey).equals(testVal)
            );
        assert(found);
    }
}