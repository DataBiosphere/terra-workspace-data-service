package org.databiosphere.workspacedataservice.activitylog;

import org.databiosphere.workspacedataservice.dao.CachedQueryDao;
import org.databiosphere.workspacedataservice.dao.DataSourceConfig;
import org.databiosphere.workspacedataservice.dao.MockInstanceDaoConfig;
import org.databiosphere.workspacedataservice.dao.RecordDao;
import org.databiosphere.workspacedataservice.datarepo.DataRepoConfig;
import org.databiosphere.workspacedataservice.sam.MockSamClientFactoryConfig;
import org.databiosphere.workspacedataservice.sam.SamConfig;
import org.databiosphere.workspacedataservice.service.BatchWriteService;
import org.databiosphere.workspacedataservice.service.DataRepoService;
import org.databiosphere.workspacedataservice.service.DataTypeInfererConfig;
import org.databiosphere.workspacedataservice.service.InstanceService;
import org.databiosphere.workspacedataservice.service.JsonConfig;
import org.databiosphere.workspacedataservice.service.RecordOrchestratorService;
import org.databiosphere.workspacedataservice.service.RecordService;
import org.databiosphere.workspacedataservice.shared.model.RecordAttributes;
import org.databiosphere.workspacedataservice.shared.model.RecordRequest;
import org.databiosphere.workspacedataservice.shared.model.RecordType;
import org.databiosphere.workspacedataservice.tsv.TsvConfig;
import org.databiosphere.workspacedataservice.workspacemanager.WorkspaceManagerConfig;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.system.CapturedOutput;
import org.springframework.boot.test.system.OutputCaptureExtension;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.ActiveProfiles;

import java.util.List;
import java.util.Optional;
import java.util.UUID;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;

@ActiveProfiles(profiles = { "mock-sam" })
@DirtiesContext
@SpringBootTest
@ExtendWith(OutputCaptureExtension.class)
public class LogStatementTest {

    private final String VERSION = "v0.2";

    @Autowired
    InstanceService instanceService;
    @Autowired
    RecordOrchestratorService recordOrchestratorService;

    @AfterEach
    void afterEach() {
        List<UUID> allInstances = instanceService.listInstances(VERSION);
        for (UUID id : allInstances) {
            instanceService.deleteInstance(id, VERSION);
        }
    }

    @Test
    void createAndDeleteInstanceLogging(CapturedOutput output) {
        UUID instanceId = UUID.randomUUID();
        instanceService.createInstance(instanceId, VERSION);
        instanceService.deleteInstance(instanceId, VERSION);
        assertThat(output.getOut())
                .contains("user anonymous created 1 instance(s) with id(s) [%s]".formatted(instanceId));
        assertThat(output.getOut())
                .contains("user anonymous deleted 1 instance(s) with id(s) [%s]".formatted(instanceId));
    }

    @Test
    void upsertRecordLogging(CapturedOutput output) {
        UUID instanceId = UUID.randomUUID();
        RecordType recordType = RecordType.valueOf("mytype");
        String recordId = "my-record-id";
        instanceService.createInstance(instanceId, VERSION);
        // loop twice - this creates the record and then updates it,
        // using the same upsert method.
        for(int i = 0; i<=1; i++) {
            recordOrchestratorService.upsertSingleRecord(instanceId, VERSION,
                    recordType, recordId,
                    Optional.empty(),
                    new RecordRequest(RecordAttributes.empty()));
        }
        assertThat(output.getOut())
                .contains("user anonymous created 1 record(s) of type %s with id(s) [%s]"
                        .formatted(recordType.getName(), recordId));
        assertThat(output.getOut())
                .contains("user anonymous updated 1 record(s) of type %s with id(s) [%s]"
                        .formatted(recordType.getName(), recordId));
    }

    @Test
    void updateRecordLogging(CapturedOutput output) {
        UUID instanceId = UUID.randomUUID();
        RecordType recordType = RecordType.valueOf("mytype");
        String recordId = "my-record-id";
        instanceService.createInstance(instanceId, VERSION);
        // create the record
        recordOrchestratorService.upsertSingleRecord(instanceId, VERSION,
                recordType, recordId,
                Optional.empty(),
                new RecordRequest(RecordAttributes.empty()));
        // now update the record - this is the method under test
        recordOrchestratorService.updateSingleRecord(instanceId, VERSION,
                recordType, recordId,
                new RecordRequest(RecordAttributes.empty()));
        assertThat(output.getOut())
                .contains("user anonymous updated 1 record(s) of type %s with id(s) [%s]"
                        .formatted(recordType.getName(), recordId));
    }

    @Test
    void deleteRecordLogging(CapturedOutput output) {
        UUID instanceId = UUID.randomUUID();
        RecordType recordType = RecordType.valueOf("mytype");
        String recordId = "my-record-id";
        instanceService.createInstance(instanceId, VERSION);
        // create the record
        recordOrchestratorService.upsertSingleRecord(instanceId, VERSION,
                recordType, recordId,
                Optional.empty(),
                new RecordRequest(RecordAttributes.empty()));
        // now delete the record - this is the method under test
        recordOrchestratorService.deleteSingleRecord(instanceId, VERSION,
                recordType, recordId);
        assertThat(output.getOut())
                .contains("user anonymous deleted 1 record(s) of type %s with id(s) [%s]"
                        .formatted(recordType.getName(), recordId));
    }

    @Test
    void deleteRecordTypeLogging(CapturedOutput output) {
        UUID instanceId = UUID.randomUUID();
        RecordType recordType = RecordType.valueOf("mytype");
        String recordId = "my-record-id";
        instanceService.createInstance(instanceId, VERSION);
        // create the record
        recordOrchestratorService.upsertSingleRecord(instanceId, VERSION,
                recordType, recordId,
                Optional.empty(),
                new RecordRequest(RecordAttributes.empty()));
        // now delete the entire record type - this is the method under test
        recordOrchestratorService.deleteRecordType(instanceId, VERSION,
                recordType);
        assertThat(output.getOut())
                .contains("user anonymous deleted 1 table(s) of type %s"
                        .formatted(recordType.getName()));
    }

    // TODO: tsv upload
    // TODO: batch upload
    // TODO: link snapshot




}
