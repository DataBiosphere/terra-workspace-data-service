package org.databiosphere.workspacedataservice.controller;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import com.fasterxml.jackson.databind.ObjectMapper;
import jakarta.annotation.Nullable;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import org.apache.commons.lang3.RandomStringUtils;
import org.databiosphere.workspacedataservice.TestUtils;
import org.databiosphere.workspacedataservice.dao.WorkspaceRepository;
import org.databiosphere.workspacedataservice.service.CollectionService;
import org.databiosphere.workspacedataservice.service.RecordOrchestratorService;
import org.databiosphere.workspacedataservice.shared.model.RecordAttributes;
import org.databiosphere.workspacedataservice.shared.model.RecordQueryResponse;
import org.databiosphere.workspacedataservice.shared.model.RecordRequest;
import org.databiosphere.workspacedataservice.shared.model.RecordResponse;
import org.databiosphere.workspacedataservice.shared.model.RecordType;
import org.databiosphere.workspacedataservice.shared.model.WorkspaceId;
import org.databiosphere.workspacedataservice.workspace.WorkspaceDataTableType;
import org.databiosphere.workspacedataservice.workspace.WorkspaceRecord;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.MediaType;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.web.servlet.MvcResult;

@ActiveProfiles(profiles = "mock-sam")
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class RecordControllerSearchFilterTest extends MockMvcTestBase {

  @Nullable private UUID collectionId;

  private static final String VERSION = "v0.2";
  private static final RecordType RECTYPE = RecordType.valueOf("mytype");

  @Autowired CollectionService collectionService;
  @Autowired NamedParameterJdbcTemplate namedTemplate;
  @Autowired RecordOrchestratorService recordOrchestratorService;
  @Autowired ObjectMapper mapper;
  @Autowired WorkspaceRepository workspaceRepository;

  // all tests in this class use the same set of data; all tests in this class are read-only.
  // as setup, insert 20 records with ids "000" through "019". Each record has a "sortByMe"
  // attribute using the same "000"-"019" value to ensure deterministic sorting.
  @BeforeAll
  void beforeAll() {
    WorkspaceId workspaceId = WorkspaceId.of(UUID.randomUUID());

    // create the workspace record
    workspaceRepository.save(
        new WorkspaceRecord(workspaceId, WorkspaceDataTableType.WDS, /* newFlag= */ true));

    // create collection
    collectionId =
        collectionService.save(workspaceId, RandomStringUtils.randomAlphabetic(16), "desc").getId();
    // insert 20 records into the collection
    for (int i = 0; i < 20; i++) {
      // for easy sorting:
      String leadingZeroes = String.format("%03d", i);

      recordOrchestratorService.upsertSingleRecord(
          collectionId,
          VERSION,
          RECTYPE,
          leadingZeroes,
          Optional.empty(),
          new RecordRequest(new RecordAttributes(Map.of("sortByMe", leadingZeroes))));
    }
  }

  @AfterAll
  void deleteAllInstances() {
    TestUtils.cleanAllCollections(collectionService, namedTemplate);
    TestUtils.cleanAllWorkspaces(namedTemplate);
  }

  @Test
  void nullFilter() {
    String testCase = "when filter is entirely omitted";
    String searchRequestPayload = """
{"limit": 5, "sortAttribute": "sortByMe"}
""";
    List<String> expected = List.of("000", "001", "002", "003", "004");
    executeTest(searchRequestPayload, expected, testCase);
  }

  @Test
  void nullIds() {
    String testCase = "when filter is specified but filter.ids is not";
    String searchRequestPayload = """
{"limit": 5, "sortAttribute": "sortByMe", "filter": {}}
""";
    List<String> expected = List.of("000", "001", "002", "003", "004");
    executeTest(searchRequestPayload, expected, testCase);
  }

  @Test
  void emptyIds() {
    String testCase = "when filter.ids is specified but empty";
    String searchRequestPayload =
        """
{"limit": 5, "sortAttribute": "sortByMe", "filter": {"ids": []}}
""";
    List<String> expected = List.of();
    executeTest(searchRequestPayload, expected, testCase);
  }

  @Test
  void oneId() {
    String testCase = "when filter.ids contains a single matching id";
    String searchRequestPayload =
        """
{"limit": 5, "sortAttribute": "sortByMe", "filter": {"ids": ["012"]}}
""";
    List<String> expected = List.of("012");
    executeTest(searchRequestPayload, expected, testCase);
  }

  @Test
  void multipleIds() {
    String testCase = "when filter.ids contains multiple matching ids";
    String searchRequestPayload =
        """
{"limit": 5, "sortAttribute": "sortByMe", "filter": {"ids": ["003", "009", "012"]}}
""";
    List<String> expected = List.of("003", "009", "012");
    executeTest(searchRequestPayload, expected, testCase);
  }

  @Test
  void someMatch() {
    String testCase = "when filter.ids contains some known and some unknown ids";
    String searchRequestPayload =
        """
{"limit": 5, "sortAttribute": "sortByMe", "filter": {"ids": ["003", "unknown", "012"]}}
""";
    List<String> expected = List.of("003", "012");
    executeTest(searchRequestPayload, expected, testCase);
  }

  @Test
  void unknownIds() {
    String testCase = "when filter.ids contains ids that match nothing";
    String searchRequestPayload =
        """
{"limit": 5, "sortAttribute": "sortByMe", "filter": {"ids": ["unknown1", "unknown2"]}}
""";
    List<String> expected = List.of();
    executeTest(searchRequestPayload, expected, testCase);
  }

  @Test
  void moreIdsThanLimit() {
    String testCase = "when filter.ids contains more matching ids than the query limit";
    String searchRequestPayload =
        """
{"limit": 2, "sortAttribute": "sortByMe", "filter": {"ids": ["002", "004", "006", "008"]}}
""";
    List<String> expected = List.of("002", "004");
    executeTest(searchRequestPayload, expected, testCase);
  }

  @Test
  void offsetRespected() {
    String testCase = "when filter.ids is over limit, offset is still respected";
    String searchRequestPayload =
        """
{"limit": 2, "offset": 2, "sortAttribute": "sortByMe", "filter": {"ids": ["002", "004", "006", "008"]}}
""";
    List<String> expected = List.of("006", "008");
    executeTest(searchRequestPayload, expected, testCase);
  }

  @Test
  void idOrderIsIgnored() {
    String testCase = "when filter.ids order conflicts with sortAttribute, sortAttribute wins";
    String searchRequestPayload =
        """
{"limit": 3, "sortAttribute": "sortByMe", "sort": "DESC", "filter": {"ids": ["004", "006", "008"]}}
""";
    List<String> expected = List.of("008", "006", "004");
    executeTest(searchRequestPayload, expected, testCase);
  }

  /**
   * Implementation for all test cases in this class. Executes a query using the supplied payload,
   * then validates that the returned record ids match the expected record ids.
   *
   * @param searchRequestPayload the payload to use when querying. This is specified as a String
   *     instead of as a SearchRequest to ensure we test String-to-SearchRequest deserialization as
   *     well as query functionality.
   * @param expectedRecordIds the record ids expected to be returned by the query
   * @param testCase descriptor for the test to be displayed in case of failures.
   */
  private void executeTest(
      String searchRequestPayload, List<String> expectedRecordIds, String testCase) {
    try {
      // query for records using the input searchRequest
      MvcResult mvcResult =
          mockMvc
              .perform(
                  post(
                          "/{instanceId}/search/{version}/{recordType}",
                          collectionId,
                          VERSION,
                          RECTYPE)
                      .contentType(MediaType.APPLICATION_JSON)
                      .content(searchRequestPayload))
              .andExpect(status().isOk())
              .andReturn();

      RecordQueryResponse recordQueryResponse =
          mapper.readValue(mvcResult.getResponse().getContentAsString(), RecordQueryResponse.class);

      List<RecordResponse> recordResponseList = recordQueryResponse.records();
      List<String> actualRecordIds =
          recordResponseList.stream().map(RecordResponse::recordId).toList();

      assertThat(actualRecordIds).describedAs(testCase).isEqualTo(expectedRecordIds);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }
}
