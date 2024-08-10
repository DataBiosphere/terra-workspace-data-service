package org.databiosphere.workspacedataservice.activitylog;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;

import bio.terra.datarepo.api.RepositoryApi;
import bio.terra.workspace.api.ReferencedGcpResourceApi;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import org.databiosphere.workspacedataservice.common.TestBase;
import org.databiosphere.workspacedataservice.service.CollectionService;
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
import org.springframework.test.context.TestPropertySource;
import org.springframework.web.multipart.MultipartFile;

@SpringBootTest
@ExtendWith(OutputCaptureExtension.class)
@TestPropertySource(
    properties = {
      "twds.tenancy.enforce-collections-match-workspace-id=false", // TODO(AJ-1682): get rid of this
    })
@DirtiesContext
class LogStatementTest extends TestBase {

  private final String VERSION = "v0.2";

  @Autowired CollectionService collectionService;
  @Autowired RecordOrchestratorService recordOrchestratorService;
  @Autowired ObjectMapper objectMapper;

  // mocking for Workspace Manager
  @MockBean WorkspaceManagerClientFactory mockWorkspaceManagerClientFactory;
  final ReferencedGcpResourceApi mockReferencedGcpResourceApi =
      Mockito.mock(ReferencedGcpResourceApi.class);

  // mocking for data repo
  final RepositoryApi mockRepositoryApi = Mockito.mock(RepositoryApi.class);

  @AfterEach
  void tearDown() {
    List<UUID> allCollections = collectionService.listCollections(VERSION);
    for (UUID id : allCollections) {
      collectionService.deleteCollection(id, VERSION);
    }
  }

  @Test
  void createAndDeleteCollectionLogging(CapturedOutput output) {
    UUID collectionId = UUID.randomUUID();
    collectionService.createCollection(collectionId, VERSION);
    collectionService.deleteCollection(collectionId, VERSION);
    assertThat(output.getOut())
        .contains("user anonymous created 1 collection(s) with id(s) [%s]".formatted(collectionId));
    assertThat(output.getOut())
        .contains("user anonymous deleted 1 collection(s) with id(s) [%s]".formatted(collectionId));
  }

  @Test
  void upsertRecordLogging(CapturedOutput output) {
    UUID collectionId = UUID.randomUUID();
    RecordType recordType = RecordType.valueOf("mytype");
    String recordId = "my-record-id";
    collectionService.createCollection(collectionId, VERSION);
    // loop twice - this creates the record and then updates it,
    // using the same upsert method.
    for (int i = 0; i <= 1; i++) {
      recordOrchestratorService.upsertSingleRecord(
          collectionId,
          VERSION,
          recordType,
          recordId,
          Optional.empty(),
          new RecordRequest(RecordAttributes.empty()));
    }
    assertThat(output.getOut())
        .contains(
            "user anonymous created 1 record(s) of type %s with id(s) [%s]"
                .formatted(recordType.getName(), recordId));
    assertThat(output.getOut())
        .contains(
            "user anonymous updated 1 record(s) of type %s with id(s) [%s]"
                .formatted(recordType.getName(), recordId));
  }

  @Test
  void updateRecordLogging(CapturedOutput output) {
    UUID collectionId = UUID.randomUUID();
    RecordType recordType = RecordType.valueOf("mytype");
    String recordId = "my-record-id";
    collectionService.createCollection(collectionId, VERSION);
    // create the record
    recordOrchestratorService.upsertSingleRecord(
        collectionId,
        VERSION,
        recordType,
        recordId,
        Optional.empty(),
        new RecordRequest(RecordAttributes.empty()));
    // now update the record - this is the method under test
    recordOrchestratorService.updateSingleRecord(
        collectionId, VERSION, recordType, recordId, new RecordRequest(RecordAttributes.empty()));
    assertThat(output.getOut())
        .contains(
            "user anonymous updated 1 record(s) of type %s with id(s) [%s]"
                .formatted(recordType.getName(), recordId));
  }

  @Test
  void deleteRecordLogging(CapturedOutput output) {
    UUID collectionId = UUID.randomUUID();
    RecordType recordType = RecordType.valueOf("mytype");
    String recordId = "my-record-id";
    collectionService.createCollection(collectionId, VERSION);
    // create the record
    recordOrchestratorService.upsertSingleRecord(
        collectionId,
        VERSION,
        recordType,
        recordId,
        Optional.empty(),
        new RecordRequest(RecordAttributes.empty()));
    // now delete the record - this is the method under test
    recordOrchestratorService.deleteSingleRecord(collectionId, VERSION, recordType, recordId);
    assertThat(output.getOut())
        .contains(
            "user anonymous deleted 1 record(s) of type %s with id(s) [%s]"
                .formatted(recordType.getName(), recordId));
  }

  @Test
  void deleteRecordTypeLogging(CapturedOutput output) {
    UUID collectionId = UUID.randomUUID();
    RecordType recordType = RecordType.valueOf("mytype");
    String recordId = "my-record-id";
    collectionService.createCollection(collectionId, VERSION);
    // create the record
    recordOrchestratorService.upsertSingleRecord(
        collectionId,
        VERSION,
        recordType,
        recordId,
        Optional.empty(),
        new RecordRequest(RecordAttributes.empty()));
    // now delete the entire record type - this is the method under test
    recordOrchestratorService.deleteRecordType(collectionId, VERSION, recordType);
    assertThat(output.getOut())
        .contains("user anonymous deleted 1 table(s) of type %s".formatted(recordType.getName()));
  }

  @Test
  void tsvUploadLogging(CapturedOutput output) throws IOException {
    UUID collectionId = UUID.randomUUID();
    RecordType recordType = RecordType.valueOf("mytype");
    collectionService.createCollection(collectionId, VERSION);

    try (InputStream tsvStream = ClassLoader.getSystemResourceAsStream("tsv/small-test.tsv")) {
      MultipartFile upload = new MockMultipartFile("myupload", tsvStream);
      recordOrchestratorService.tsvUpload(
          collectionId, VERSION, recordType, Optional.empty(), upload);
      assertThat(output.getOut())
          .contains(
              "user anonymous upserted 2 record(s) of type %s".formatted(recordType.getName()));
    }
  }

  @Test
  void batchWriteLogging(CapturedOutput output) throws IOException {
    UUID collectionId = UUID.randomUUID();
    RecordType recordType = RecordType.valueOf("mytype");
    collectionService.createCollection(collectionId, VERSION);

    BatchOperation[] ops =
        new BatchOperation[] {
          new BatchOperation(
              new Record("aaa", recordType, RecordAttributes.empty()), OperationType.UPSERT),
          new BatchOperation(
              new Record("bbb", recordType, RecordAttributes.empty()), OperationType.UPSERT),
          new BatchOperation(
              new Record("aaa", recordType, RecordAttributes.empty()), OperationType.DELETE)
        };

    InputStream upload = new ByteArrayInputStream(objectMapper.writeValueAsBytes(ops));

    recordOrchestratorService.streamingWrite(
        collectionId, VERSION, recordType, Optional.empty(), upload);
    assertThat(output.getOut())
        .contains("user anonymous modified 3 record(s) of type %s".formatted(recordType.getName()));
  }
}
