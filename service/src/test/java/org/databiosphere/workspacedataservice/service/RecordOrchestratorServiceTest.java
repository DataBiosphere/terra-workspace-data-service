package org.databiosphere.workspacedataservice.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.databiosphere.workspacedataservice.dao.RecordDao;
import org.databiosphere.workspacedataservice.shared.model.RecordAttributes;
import org.databiosphere.workspacedataservice.shared.model.RecordRequest;
import org.databiosphere.workspacedataservice.shared.model.RecordResponse;
import org.databiosphere.workspacedataservice.shared.model.RecordType;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;

@SpringBootTest
class RecordOrchestratorServiceTest {

    @Autowired
    private RecordDao recordDao;

    @Autowired
    private DataTypeInferer inferer;

    @Autowired
    private BatchWriteService batchWriteService;

    @Autowired
    private RecordService recordService;

    @Autowired
    private ObjectMapper objectMapper;

    @Autowired RecordOrchestratorService recordOrchestratorService;


    @Test
    void updateSingleRecord() {
        RecordType type = RecordType.valueOf("test");
        RecordRequest req = new RecordRequest(RecordAttributes.empty("primaryKey").putAttribute("test", "val"));

        RecordResponse resp =
            recordOrchestratorService.updateSingleRecord(UUID.randomUUID(), RecordOrchestratorService.VERSION, type,
                "abc", req);
    }

    @Test
    void getSingleRecord() {
    }

    @Test
    void tsvUpload() {
    }

    @Test
    void streamAllEntities() {
    }

    @Test
    void queryForRecords() {
    }

    @Test
    void upsertSingleRecord() {
    }

    @Test
    void deleteSingleRecord() {
    }

    @Test
    void deleteRecordType() {
    }

    @Test
    void describeRecordType() {
    }

    @Test
    void describeAllRecordTypes() {
    }

    @Test
    void streamingWrite() {
    }

    @Test
    void validateVersion() {
    }

    @Test
    void validateInstance() {
    }
}