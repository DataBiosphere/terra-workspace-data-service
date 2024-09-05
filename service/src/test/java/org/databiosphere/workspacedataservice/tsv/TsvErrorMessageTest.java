package org.databiosphere.workspacedataservice.tsv;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.startsWith;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.io.InputStream;
import java.util.Optional;
import java.util.UUID;
import org.databiosphere.workspacedataservice.TestUtils;
import org.databiosphere.workspacedataservice.common.ControlPlaneTestBase;
import org.databiosphere.workspacedataservice.dao.WorkspaceRepository;
import org.databiosphere.workspacedataservice.generated.CollectionServerModel;
import org.databiosphere.workspacedataservice.service.CollectionService;
import org.databiosphere.workspacedataservice.service.RecordOrchestratorService;
import org.databiosphere.workspacedataservice.service.model.exception.InvalidTsvException;
import org.databiosphere.workspacedataservice.shared.model.RecordType;
import org.databiosphere.workspacedataservice.shared.model.WorkspaceId;
import org.databiosphere.workspacedataservice.workspace.WorkspaceDataTableType;
import org.databiosphere.workspacedataservice.workspace.WorkspaceRecord;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.http.MediaType;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.mock.web.MockMultipartFile;
import org.springframework.test.annotation.DirtiesContext;

@SpringBootTest
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@DirtiesContext
class TsvErrorMessageTest extends ControlPlaneTestBase {

  @Autowired CollectionService collectionService;
  @Autowired RecordOrchestratorService recordOrchestratorService;
  @Autowired NamedParameterJdbcTemplate namedTemplate;
  @Autowired private WorkspaceRepository workspaceRepository;

  private UUID collectionId;

  private static final String VERSION = "v0.2";

  @BeforeEach
  void setUp() {
    WorkspaceId workspaceId = WorkspaceId.of(UUID.randomUUID());

    // create the workspace record
    workspaceRepository.save(
        new WorkspaceRecord(workspaceId, WorkspaceDataTableType.WDS, /* newFlag= */ true));
    CollectionServerModel collectionServerModel =
        TestUtils.createCollection(collectionService, workspaceId);
    collectionId = collectionServerModel.getId();
  }

  @AfterEach
  void tearDown() {
    TestUtils.cleanAllCollections(collectionService, namedTemplate);
    TestUtils.cleanAllWorkspaces(namedTemplate);
  }

  @ParameterizedTest(name = "The malformed TSV file '{0}' should throw a helpful error")
  @ValueSource(
      strings = {
        "tsv/errors/column-separator.tsv",
        "tsv/errors/empty-header-line.tsv",
        "tsv/errors/invalid-middle-byte.tsv",
        "tsv/errors/too-many-entries.tsv"
      })
  void testBadTsvFile(String badTsvFile) throws Exception {
    try (InputStream inputStream = ClassLoader.getSystemResourceAsStream(badTsvFile)) {

      MockMultipartFile tsvUpload =
          new MockMultipartFile("some_file", "some_file", MediaType.TEXT_PLAIN_VALUE, inputStream);

      Exception actual =
          assertThrows(
              InvalidTsvException.class,
              () ->
                  recordOrchestratorService.tsvUpload(
                      collectionId,
                      VERSION,
                      RecordType.valueOf("will_error"),
                      Optional.empty(),
                      tsvUpload));

      assertThat(
          actual.getMessage(),
          startsWith(
              "Error reading TSV. Please check the format of your upload. Underlying error is"));
    }
  }
}
