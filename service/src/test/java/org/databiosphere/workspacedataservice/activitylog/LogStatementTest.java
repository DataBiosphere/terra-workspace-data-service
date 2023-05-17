package org.databiosphere.workspacedataservice.activitylog;

import bio.terra.datarepo.api.RepositoryApi;
import bio.terra.datarepo.client.ApiException;
import bio.terra.datarepo.model.SnapshotModel;
import bio.terra.workspace.api.ReferencedGcpResourceApi;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.databiosphere.workspacedataservice.datarepo.DataRepoClientFactory;
import org.databiosphere.workspacedataservice.service.DataRepoService;
import org.databiosphere.workspacedataservice.service.InstanceService;
import org.databiosphere.workspacedataservice.service.RecordOrchestratorService;
import org.databiosphere.workspacedataservice.shared.model.BatchOperation;
import org.databiosphere.workspacedataservice.shared.model.OperationType;
import org.databiosphere.workspacedataservice.shared.model.Record;
import org.databiosphere.workspacedataservice.shared.model.RecordAttributes;
import org.databiosphere.workspacedataservice.shared.model.RecordRequest;
import org.databiosphere.workspacedataservice.shared.model.RecordType;
import org.databiosphere.workspacedataservice.workspacemanager.WorkspaceManagerClientFactory;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mockito;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.boot.test.system.CapturedOutput;
import org.springframework.boot.test.system.OutputCaptureExtension;
import org.springframework.mock.web.MockMultipartFile;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.web.multipart.MultipartFile;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.BDDMockito.given;

@ActiveProfiles(profiles = { "mock-sam" })
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_CLASS)
@SpringBootTest
@ExtendWith(OutputCaptureExtension.class)
public class LogStatementTest {

    private final String VERSION = "v0.2";

    @Autowired
    InstanceService instanceService;
    @Autowired
    RecordOrchestratorService recordOrchestratorService;
    @Autowired
    DataRepoService dataRepoService;
    @Autowired
    ObjectMapper objectMapper;

    // mocking for Workspace Manager
    @MockBean
    WorkspaceManagerClientFactory mockWorkspaceManagerClientFactory;
    ReferencedGcpResourceApi mockReferencedGcpResourceApi = Mockito.mock(ReferencedGcpResourceApi.class);

    // mocking for data repo
    @MockBean
    DataRepoClientFactory mockDataRepoClientFactory;
    RepositoryApi mockRepositoryApi = Mockito.mock(RepositoryApi.class);

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

    @Test
    void tsvUploadLogging(CapturedOutput output) throws IOException {
        UUID instanceId = UUID.randomUUID();
        RecordType recordType = RecordType.valueOf("mytype");
        instanceService.createInstance(instanceId, VERSION);

        try (InputStream tsvStream = ClassLoader.getSystemResourceAsStream("small-test.tsv")) {
            MultipartFile upload = new MockMultipartFile("myupload", tsvStream);
            recordOrchestratorService.tsvUpload(instanceId, VERSION,
                    recordType, Optional.empty(),
                    upload);
            assertThat(output.getOut())
                    .contains("user anonymous upserted 2 record(s) of type %s"
                            .formatted(recordType.getName()));
        }
    }

    @Test
    void batchWriteLogging(CapturedOutput output) throws IOException {
        UUID instanceId = UUID.randomUUID();
        RecordType recordType = RecordType.valueOf("mytype");
        instanceService.createInstance(instanceId, VERSION);

        BatchOperation[] ops = new BatchOperation[]{
                new BatchOperation(new Record("aaa", recordType, RecordAttributes.empty()),
                        OperationType.UPSERT),
                new BatchOperation(new Record("bbb", recordType, RecordAttributes.empty()),
                        OperationType.UPSERT),
                new BatchOperation(new Record("aaa", recordType, RecordAttributes.empty()),
                        OperationType.DELETE)
        };

        InputStream upload = new ByteArrayInputStream(objectMapper.writeValueAsBytes(ops));

        recordOrchestratorService.streamingWrite(instanceId, VERSION,
                recordType, Optional.empty(),
                upload);
        assertThat(output.getOut())
                .contains("user anonymous modified 3 record(s) of type %s"
                        .formatted(recordType.getName()));

    }

    @Test
    void importSnapshotLogging(CapturedOutput output) throws ApiException {
        UUID instanceId = UUID.randomUUID();
        instanceService.createInstance(instanceId, VERSION);

        UUID snapshotId = UUID.randomUUID();

        given(mockWorkspaceManagerClientFactory.getReferencedGcpResourceApi()).willReturn(mockReferencedGcpResourceApi);
        given(mockDataRepoClientFactory.getRepositoryApi()).willReturn(mockRepositoryApi);

        final SnapshotModel testSnapshot = new SnapshotModel().name("test snapshot").id(snapshotId);
        given(mockRepositoryApi.retrieveSnapshot(any(), any()))
                .willReturn(testSnapshot);


        dataRepoService.importSnapshot(instanceId, snapshotId);
        assertThat(output.getOut())
                .contains("user anonymous linked 1 snapshot reference(s) with id(s) [%s]"
                        .formatted(snapshotId));
    }
}
