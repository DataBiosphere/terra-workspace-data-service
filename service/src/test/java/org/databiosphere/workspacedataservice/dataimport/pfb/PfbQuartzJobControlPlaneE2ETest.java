package org.databiosphere.workspacedataservice.dataimport.pfb;

import static java.util.Arrays.stream;
import static java.util.stream.Collectors.counting;
import static java.util.stream.Collectors.groupingBy;
import static org.assertj.core.api.Assertions.assertThat;
import static org.databiosphere.workspacedataservice.TestTags.SLOW;
import static org.databiosphere.workspacedataservice.dataimport.rawlsjson.RawlsJsonQuartzJob.rawlsJsonBlobName;
import static org.databiosphere.workspacedataservice.recordsink.RawlsModel.AttributeOperation;
import static org.databiosphere.workspacedataservice.recordsink.RawlsModel.Entity;
import static org.databiosphere.workspacedataservice.recordsink.RawlsModel.Op;
import static org.databiosphere.workspacedataservice.recordsink.RawlsModel.Op.ADD_LIST_MEMBER;
import static org.databiosphere.workspacedataservice.recordsink.RawlsModel.Op.ADD_UPDATE_ATTRIBUTE;
import static org.databiosphere.workspacedataservice.recordsink.RawlsModel.Op.CREATE_ATTRIBUTE_VALUE_LIST;
import static org.databiosphere.workspacedataservice.recordsink.RawlsModel.Op.REMOVE_ATTRIBUTE;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.collect.ImmutableMap;
import com.google.mu.util.stream.BiStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.regex.Pattern;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;
import org.databiosphere.workspacedataservice.annotations.WithTestObservationRegistry;
import org.databiosphere.workspacedataservice.dataimport.ImportValidator;
import org.databiosphere.workspacedataservice.pubsub.PubSub;
import org.databiosphere.workspacedataservice.rawls.RawlsClient;
import org.databiosphere.workspacedataservice.rawls.SnapshotListResponse;
import org.databiosphere.workspacedataservice.recordsink.RawlsModel.AddListMember;
import org.databiosphere.workspacedataservice.recordsink.RawlsModel.AddUpdateAttribute;
import org.databiosphere.workspacedataservice.recordsink.RawlsModel.AttributeValue;
import org.databiosphere.workspacedataservice.recordsink.RawlsModel.CreateAttributeValueList;
import org.databiosphere.workspacedataservice.recordsink.RawlsModel.EntityReference;
import org.databiosphere.workspacedataservice.recordsink.RawlsModel.RemoveAttribute;
import org.databiosphere.workspacedataservice.sam.MockSamUsersApi;
import org.databiosphere.workspacedataservice.sam.SamDao;
import org.databiosphere.workspacedataservice.service.CollectionService;
import org.databiosphere.workspacedataservice.shared.model.BearerToken;
import org.databiosphere.workspacedataservice.shared.model.RecordType;
import org.databiosphere.workspacedataservice.storage.GcsStorage;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.quartz.JobExecutionException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.boot.test.mock.mockito.SpyBean;
import org.springframework.core.io.Resource;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.MvcResult;
import org.springframework.util.StreamUtils;

/**
 * Tests for PFB import that execute "end-to-end" - that is, they go through the whole process of
 * parsing the PFB, and generating the JSON that gets stored in a bucket and communicated to Rawls
 * via pubsub.
 */
@ActiveProfiles(profiles = {"mock-sam", "noop-scheduler-dao", "control-plane"})
@DirtiesContext
@SpringBootTest
@TestPropertySource(
    properties = {
      // turn off pubsub autoconfiguration for tests
      "spring.cloud.gcp.pubsub.enabled=false",
      // Rawls url must be valid, else context initialization (Spring startup) will fail
      "rawlsUrl=https://localhost/",
      "management.prometheus.metrics.export.enabled=true"
    })
@WithTestObservationRegistry
@AutoConfigureMockMvc
class PfbQuartzJobControlPlaneE2ETest {
  @Autowired ObjectMapper mapper;
  @Autowired CollectionService collectionService;
  @Autowired PfbTestSupport testSupport;
  @Autowired MockMvc mockMvc;

  @Autowired
  @Qualifier("mockGcsStorage")
  GcsStorage storage;

  @SpyBean PubSub pubSub;
  @SpyBean SamDao samDao;
  // Mock ImportValidator to allow importing test data from a file:// URL.
  @MockBean ImportValidator importValidator;
  @MockBean RawlsClient rawlsClient;

  /** ArgumentCaptor for the message passed to {@link PubSub#publishSync(Map)}. */
  @Captor private ArgumentCaptor<Map<String, String>> pubSubMessageCaptor;

  @Value("classpath:pfb/minimal-data.pfb")
  Resource minimalDataPfb;

  @Value("classpath:batch-write-rawls/from-minimal-data-pfb.json")
  Resource minimalDataExpectedJson;

  @Value("classpath:pfb/data-with-array.pfb")
  Resource dataWithArrayPfb;

  @Value("classpath:batch-write-rawls/from-data-with-array-pfb.json")
  Resource dataWithArrayExpectedJson;

  @Value("classpath:avro/test.avro")
  Resource testAvroWithMultipleBatches;

  @Value("classpath:pfb/array-of-json.avro")
  Resource dataWithArrayOfJson;

  private UUID collectionId;

  @BeforeEach
  void setup() {
    collectionId = UUID.randomUUID();

    // stub out rawls to report no snapshots already linked to this workspace
    when(rawlsClient.enumerateDataRepoSnapshotReferences(any(), anyInt(), anyInt()))
        .thenReturn(new SnapshotListResponse(Collections.emptyList()));
  }

  @AfterEach
  void teardown() {
    storage.getBlobsInBucket().forEach(blob -> storage.deleteBlob(blob.getName()));
  }

  /* import test.avro, and validate the tables and row counts it imported. */
  @Test
  @Tag(SLOW)
  void pfbToRawlsEntity() throws JobExecutionException, IOException {
    // Arrange / Act
    UUID jobId = testSupport.executePfbImportQuartzJob(collectionId, minimalDataPfb);

    // Assert
    assertPubSubMessage(expectedPubSubMessageFor(jobId));
    assertSingleBlobWritten(rawlsJsonBlobName(jobId));
    InputStream writtenJson = storage.getBlobContents(rawlsJsonBlobName(jobId));

    var entities = assertRecordedEntitiesSerde(writtenJson, minimalDataExpectedJson);
    assertThat(entities.size()).isEqualTo(1);
    var entity = entities.get(0);
    assertThat(entity.name()).isEqualTo("HG01101_cram");
    assertThat(entity.entityType()).isEqualTo("submitted_aligned_reads");

    int totalAttributes = 19; // based on the contents of the input file
    int nullAttributes = 3; // based on the contents of the input file
    int expectedAttributes = totalAttributes - nullAttributes;
    assertThat(entity.operations().size()).isEqualTo(expectedAttributes);

    assertSimpleAttributeValue(entity, "pfb:md5sum", "bdf121aadba028d57808101cb4455fa7");
    assertSimpleAttributeValue(entity, "pfb:file_size", 512);
    assertSimpleAttributeValue(entity, "pfb:file_state", "registered");
    assertSimpleAttributeValue(
        entity,
        "pfb:ga4gh_drs_uri",
        "drs://example.org/dg.4503/cc32d93d-a73c-4d2c-a061-26c0410e74fa");
  }

  /** Pattern to match a prometheus scrape string with the given name and value. */
  private static void assertMetric(
      Collection<String> metrics,
      String metricName,
      String metricValue,
      Map<String, String> expectedTags) {

    String matchingMetric =
        metrics.stream()
            .filter(
                metric ->
                    Pattern.compile("^%s\\{.*?} %s$".formatted(metricName, metricValue))
                        .matcher(metric)
                        .matches())
            .findAny()
            .orElseThrow(() -> new AssertionError("No matching metric found"));

    BiStream.from(expectedTags)
        .mapToObj("%s=\"%s\""::formatted)
        .forEach(expectedRegex -> assertThat(matchingMetric).contains(expectedRegex));
  }

  private static void assertMetric(
      Collection<String> metrics, String metricName, String metricValue) {
    assertMetric(metrics, metricName, metricValue, /* expectedTags= */ Map.of());
  }

  @Test
  @Tag(SLOW)
  void pfbToRawlsEntityWithArrayOfStrings() throws JobExecutionException, IOException {
    // Arrange / Act
    UUID jobId = testSupport.executePfbImportQuartzJob(collectionId, dataWithArrayPfb);

    // Assert
    assertPubSubMessage(expectedPubSubMessageFor(jobId));
    assertSingleBlobWritten(rawlsJsonBlobName(jobId));

    InputStream writtenJson = storage.getBlobContents(rawlsJsonBlobName(jobId));
    var entities = assertRecordedEntitiesSerde(writtenJson, dataWithArrayExpectedJson);

    assertThat(entities.size()).isEqualTo(1);
    var entity = entities.get(0);
    assertThat(entity.name()).isEqualTo("HG01101_cram");
    assertThat(entity.entityType()).isEqualTo("submitted_aligned_reads");
    assertThat(entity.operations().size()).isEqualTo(20);

    // we should have 1 RemoveAttribute, 1 CreateAttributeValueList, and 3 AddListMembers
    assertThat(filteredOps(entity, REMOVE_ATTRIBUTE).count()).isEqualTo(1);
    assertThat(filteredOps(entity, CREATE_ATTRIBUTE_VALUE_LIST).count()).isEqualTo(1);
    assertThat(filteredOps(entity, ADD_LIST_MEMBER).count()).isEqualTo(3);

    // finally, let's validate that these operations occur in the proper order,
    // that they all work on the 'file_state' property,
    // and that the AddListMembers have the correct values
    var arrayProp = "pfb:file_state";
    var startIndex = entity.operations().indexOf(new RemoveAttribute(arrayProp));
    var actual = entity.operations().subList(startIndex, startIndex + 5);
    var expected =
        List.of(
            new RemoveAttribute(arrayProp),
            new CreateAttributeValueList(arrayProp),
            new AddListMember(arrayProp, AttributeValue.of("00000000-0000-0000-0000-000000000000")),
            new AddListMember(arrayProp, AttributeValue.of("11111111-1111-1111-1111-111111111111")),
            new AddListMember(
                arrayProp, AttributeValue.of("22222222-2222-2222-2222-222222222222")));
    assertThat(actual).isEqualTo(expected);
  }

  @Test
  @Tag(SLOW)
  void pfbToRawlsEntityWithArrayOfJson() throws JobExecutionException, IOException {
    // Arrange / Act
    // "dataWithArrayOfJson" contains two rows; each row has a "links" attribute which is an
    // array of json objects.
    UUID jobId = testSupport.executePfbImportQuartzJob(collectionId, dataWithArrayOfJson);

    // Assert
    assertPubSubMessage(expectedPubSubMessageFor(jobId));
    assertSingleBlobWritten(rawlsJsonBlobName(jobId));
    /*
     When serialized properly, the array of json will look like this. Note that the "newMember"
     value is an OBJECT:
     {"op":"AddListMember" ... "newMember":{"inputs":[{"input_id":"550e8400-e29b-41d4-a716-446655440000","input_type":"cell_suspension"}],"link_type":

     When serialized improperly, with extra escaping, the array of json will look like this. Note
     that the "newMember" value is a STRING, containing an escaped version of the object:
     {"op":"AddListMember" ... "newMember":"{\"inputs\": [{\"input_id\": \"550e8400-e29b-41d4-a716-446655440000\", \"input_type\": \"cell_suspension\"}], \"link_type\":

     So, we can perform a simple test here that looks for \" in the written json.

     N.B. attempting to deserialize the written json into AttributeOperation / AddListMember objects
     for testing is tricky - when deserializing into a String, the ObjectMapper will treat both of
     the above as the same value. The test below bypasses deserialization to ensure we're looking
     at raw values.
    */
    InputStream writtenJson = storage.getBlobContents(rawlsJsonBlobName(jobId));
    // never readAllBytes() for large streams; we know this unit test's stream is a manageable size
    String writtenJsonString = new String(writtenJson.readAllBytes());
    assertThat(writtenJsonString)
        .withFailMessage("Written output should not contain escaped quotes")
        .doesNotContain("\\\"");

    // read as json
    JsonNode jsonNode = mapper.readTree(writtenJsonString);
    ArrayNode arrayNode = assertInstanceOf(ArrayNode.class, jsonNode);

    // expect 2 entities in this output
    List<ObjectNode> entityNodes =
        StreamSupport.stream(arrayNode.spliterator(), false)
            .map(objNode -> assertInstanceOf(ObjectNode.class, objNode))
            .toList();
    assertEquals(2, entityNodes.size());

    // loop through entities in the output
    entityNodes.forEach(
        entityNode -> {
          // filter to only the AddListMember ops for the "pfb:links" attribute
          ArrayNode operations = assertInstanceOf(ArrayNode.class, entityNode.get("operations"));
          List<ObjectNode> addListMemberObjects =
              new java.util.ArrayList<>(
                  StreamSupport.stream(operations.spliterator(), false)
                      .map(objNode -> assertInstanceOf(ObjectNode.class, objNode))
                      .filter(
                          objNode ->
                              objNode.has("attributeListName")
                                  && "pfb:links".equals(objNode.get("attributeListName").asText())
                                  && "AddListMember".equals(objNode.get("op").asText()))
                      .toList());
          // we should have at least one AddListMember op
          assertThat(addListMemberObjects.size()).isGreaterThan(1);
          // and the AddListMember's "newMember" value should be JSON (not a string)
          addListMemberObjects.forEach(
              addListMemberObj -> {
                assertInstanceOf(
                    ObjectNode.class,
                    addListMemberObj.get("newMember"),
                    "newMember value should be JSON");
              });
        });
  }

  @Test
  @Tag(SLOW)
  void pfbToRawlsWithMultipleBatches() throws JobExecutionException, IOException {
    // Arrange / Act
    UUID jobId = testSupport.executePfbImportQuartzJob(collectionId, testAvroWithMultipleBatches);
    Map<String, Long> expectedCounts =
        new ImmutableMap.Builder<String, Long>()
            .put("activities", 3202L)
            .put("files", 3202L * 2) // relations get 2 passes, so there will be 2 upserts each
            .put("donors", 3202L)
            .put("biosamples", 3202L)
            .put("datasets", 1L)
            .build();

    // Assert
    assertPubSubMessage(expectedPubSubMessageFor(jobId));
    assertSingleBlobWritten(rawlsJsonBlobName(jobId));
    InputStream jsonStream = storage.getBlobContents(rawlsJsonBlobName(jobId));

    List<Entity> entities = mapper.readValue(jsonStream, new TypeReference<>() {});

    // spot check some sample types
    Entity sampleActivity = getSampleEntity(entities, "activities");
    assertListAttributeType(sampleActivity, "pfb:activity_type", String.class);

    // since references get processed last, we reverse the order to get one of the reference
    // upserts as a sample instead of the original base attribute upsert
    Collections.reverse(entities);
    Entity sampleFile = getSampleEntity(entities, "files");
    assertRelationAttributeType(sampleFile, "pfb:donors", "donors");
    Map<String, Long> actualCounts =
        entities.stream().collect(groupingBy(Entity::entityType, counting()));

    assertThat(actualCounts).isEqualTo(expectedCounts);
  }

  @Test
  @Tag(SLOW)
  void pubSubRequestsUserEmail() throws JobExecutionException, IOException {
    // Arrange / Act
    testSupport.executePfbImportQuartzJob(collectionId, minimalDataPfb);

    // importing a PFB requires publishing to pubsub. As part of generating the pubsub message,
    // we query Sam for the user's email. Verify we made that call correctly.
    // the PfbTestUtils.BEARER_TOKEN is expected here because the job context was created from
    // PfbTestUtils.stubJobContext().
    verify(samDao, times(1)).getUserEmail(BearerToken.of(PfbTestUtils.BEARER_TOKEN));
  }

  @Test
  @Tag(SLOW)
  void jobSuccessMetrics() throws Exception {
    // Arrange / Act
    ImmutableMap<String, String> successTags =
        new ImmutableMap.Builder<String, String>()
            .put("jobType", "DATA_IMPORT")
            .put("importType", "PFB")
            .put("outcome", "RUNNING")
            .put("error", "none")
            .build();

    testSupport.executePfbImportQuartzJob(collectionId, minimalDataPfb);

    // Assert
    List<String> metrics = getWdsMetrics();

    // by the time we get here, there are no currently running jobs and so the _active_ metrics
    // should all be zero.
    assertMetric(metrics, "wds_job_execute_active_seconds_count", "0");
    assertMetric(metrics, "wds_job_execute_active_seconds_sum", "0.0");
    assertMetric(metrics, "wds_job_execute_active_seconds_max", "0.0");

    // (counter) we should have counted one job.execute event regardless of outcome
    assertMetric(
        metrics,
        "wds_job_execute_job_running_total",
        "1.0",
        new ImmutableMap.Builder<String, String>()
            .put("jobType", "DATA_IMPORT")
            .put("importType", "PFB")
            .build());

    // (counter) of job.execute events
    assertMetric(metrics, "wds_job_execute_seconds_count", "1", successTags);

    // (summary) sum of all job durations, and when divided by count, can get the average
    assertMetric(metrics, "wds_job_execute_seconds_sum", ".*", successTags);

    // (gauge) maximum duration tracked
    assertMetric(metrics, "wds_job_execute_seconds_max", ".*", successTags);
  }

  @Test
  @Tag(SLOW)
  void jobFailureMetrics() throws Exception {
    // Arrange / Act
    ImmutableMap<String, String> failureTags =
        new ImmutableMap.Builder<String, String>()
            .put("jobType", "DATA_IMPORT")
            .put("importType", "PFB")
            .put("outcome", "ERROR")
            .put("error", "RuntimeException")
            .build();
    when(rawlsClient.enumerateDataRepoSnapshotReferences(any(), anyInt(), anyInt()))
        .thenThrow(new RuntimeException("Fake exception for unit test"));
    testSupport.executePfbImportQuartzJob(collectionId, minimalDataPfb);

    // Assert
    List<String> metrics = getWdsMetrics();

    // by the time we get here, there are no currently running jobs and so the _active_ metrics
    // should all be zero.
    assertMetric(metrics, "wds_job_execute_active_seconds_count", "0");
    assertMetric(metrics, "wds_job_execute_active_seconds_sum", "0.0");
    assertMetric(metrics, "wds_job_execute_active_seconds_max", "0.0");

    // (counter) we should have counted one job.execute event regardless of outcome
    assertMetric(
        metrics,
        "wds_job_execute_job_running_total",
        "1.0",
        new ImmutableMap.Builder<String, String>()
            .put("jobType", "DATA_IMPORT")
            .put("importType", "PFB")
            .build());

    // (counter) of job.execute events
    assertMetric(metrics, "wds_job_execute_seconds_count", "1", failureTags);

    // (summary) sum of all job durations, and when divided by count, can get the average
    assertMetric(metrics, "wds_job_execute_seconds_sum", ".*", failureTags);

    // (gauge) maximum duration tracked
    assertMetric(metrics, "wds_job_execute_seconds_max", ".*", failureTags);
  }

  private ImmutableMap<String, String> expectedPubSubMessageFor(UUID jobId) {
    return new ImmutableMap.Builder<String, String>()
        .put("workspaceId", collectionId.toString())
        .put("userEmail", MockSamUsersApi.MOCK_USER_EMAIL)
        .put("jobId", jobId.toString())
        .put("upsertFile", storage.getBucketName() + "/" + rawlsJsonBlobName(jobId))
        .put("isUpsert", "true")
        .put("isCWDS", "true")
        .build();
  }

  private void assertPubSubMessage(Map<String, String> expectedMessage) {
    verify(pubSub).publishSync(pubSubMessageCaptor.capture());
    assertThat(pubSubMessageCaptor.getValue()).isEqualTo(expectedMessage);
  }

  private void assertSingleBlobWritten(String expectedBlobName) {
    var blobsWritten =
        StreamSupport.stream(storage.getBlobsInBucket().spliterator(), /* parallel= */ false)
            .toList();
    assertThat(blobsWritten).hasSize(1);
    assertThat(blobsWritten.get(0).getName()).isEqualTo(expectedBlobName);
  }

  private void assertSimpleAttributeValue(Entity entity, String attributeName, Object expected) {
    assertThat(getSimpleAttributeByName(entity, attributeName).addUpdateAttribute().value())
        .isEqualTo(expected);
  }

  private void assertRelationAttributeType(
      Entity entity, String attributeName, String attributeType) {
    var value = getSimpleAttributeByName(entity, attributeName).addUpdateAttribute().value();
    EntityReference entityReference = assertInstanceOf(EntityReference.class, value);

    assertThat(entityReference.entityType()).isEqualTo(RecordType.valueOf(attributeType));
    assertThat(entityReference.entityName()).startsWith("%s.".formatted(attributeType));
  }

  private AddUpdateAttribute getSimpleAttributeByName(Entity entity, String attributeName) {
    return filteredOps(entity, ADD_UPDATE_ATTRIBUTE)
        .map(AddUpdateAttribute.class::cast)
        .filter(attr -> attr.attributeName().equals(attributeName))
        .findFirst()
        .orElseThrow(
            () ->
                new AssertionError(
                    "AddUpdateAttribute %s not found in entity %s"
                        .formatted(attributeName, entity)));
  }

  private void assertListAttributeType(Entity entity, String attributeName, Class<?> expected) {
    assertThat(getListAttributeByName(entity, attributeName).newMember().value().getClass())
        .isEqualTo(expected);
  }

  private AddListMember getListAttributeByName(Entity entity, String attributeName) {
    return filteredOps(entity, ADD_LIST_MEMBER)
        .map(AddListMember.class::cast)
        .filter(attr -> attr.attributeListName().equals(attributeName))
        .findFirst()
        .orElseThrow(
            () ->
                new AssertionError(
                    "AddListMember attribute %s not found in entity %s"
                        .formatted(attributeName, entity)));
  }

  private Stream<? extends AttributeOperation> filteredOps(Entity entity, Op operation) {
    return entity.operations().stream().filter(op -> op.op() == operation);
  }

  // serde == serialize then deserialize; to test the full roundtrip to/from JSON
  private List<Entity> assertRecordedEntitiesSerde(
      InputStream jsonStream, Resource expectedJsonResource) {
    try {
      String actualJson = StreamUtils.copyToString(jsonStream, StandardCharsets.UTF_8);
      String expectedJson = new String(expectedJsonResource.getInputStream().readAllBytes());
      assertJsonEquals(expectedJson, actualJson);
      return mapper.readValue(actualJson, new TypeReference<>() {});
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  // Extra check to verify the format (or attribute names) of the actual JSON haven't drifted, since
  // serde doesn't necessarily check that.
  private void assertJsonEquals(String expectedJson, String actualJson)
      throws JsonProcessingException {
    JsonNode treeExpected = mapper.readTree(expectedJson);
    JsonNode treeActual = mapper.readTree(actualJson);

    assertThat(treeActual).isEqualTo(treeExpected);
  }

  private Entity getSampleEntity(Collection<Entity> allEntities, String typeName) {
    return allEntities.stream()
        .filter(entity -> entity.entityType().equals(typeName))
        .findFirst()
        .orElseThrow();
  }

  private List<String> getWdsMetrics() throws Exception {
    MvcResult result = mockMvc.perform(get("/prometheus")).andExpect(status().isOk()).andReturn();
    String content = result.getResponse().getContentAsString();

    return stream(content.split("\n")).filter(line -> line.startsWith("wds")).toList();
  }
}
