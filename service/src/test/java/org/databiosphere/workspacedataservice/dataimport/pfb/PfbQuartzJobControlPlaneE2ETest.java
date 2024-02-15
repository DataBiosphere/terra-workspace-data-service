package org.databiosphere.workspacedataservice.dataimport.pfb;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;
import static org.databiosphere.workspacedataservice.TestTags.SLOW;
import static org.databiosphere.workspacedataservice.rawls.Model.AttributeOperation;
import static org.databiosphere.workspacedataservice.rawls.Model.Entity;
import static org.databiosphere.workspacedataservice.rawls.Model.Op;
import static org.databiosphere.workspacedataservice.rawls.Model.Op.ADD_LIST_MEMBER;
import static org.databiosphere.workspacedataservice.rawls.Model.Op.ADD_UPDATE_ATTRIBUTE;
import static org.databiosphere.workspacedataservice.rawls.Model.Op.CREATE_ATTRIBUTE_VALUE_LIST;
import static org.databiosphere.workspacedataservice.rawls.Model.Op.REMOVE_ATTRIBUTE;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.when;

import bio.terra.workspace.model.ResourceList;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.math.BigInteger;
import java.util.List;
import java.util.UUID;
import java.util.stream.Stream;
import org.databiosphere.workspacedataservice.dao.SchedulerDao;
import org.databiosphere.workspacedataservice.rawls.Model;
import org.databiosphere.workspacedataservice.rawls.Model.AddListMember;
import org.databiosphere.workspacedataservice.rawls.Model.AddUpdateAttribute;
import org.databiosphere.workspacedataservice.rawls.Model.CreateAttributeValueList;
import org.databiosphere.workspacedataservice.rawls.Model.RemoveAttribute;
import org.databiosphere.workspacedataservice.service.BatchWriteService.RecordSink;
import org.databiosphere.workspacedataservice.service.CollectionService;
import org.databiosphere.workspacedataservice.service.RawlsRecordSink;
import org.databiosphere.workspacedataservice.workspacemanager.WorkspaceManagerDao;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.quartz.JobExecutionException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.core.io.Resource;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.ActiveProfiles;

/**
 * Tests for PFB import that execute "end-to-end" - that is, they go through the whole process of
 * parsing the PFB, and generating the JSON that (will eventually be) stored in a bucket (AJ-1585)
 * and communicated to Rawls via pubsub (AJ-1586).
 */
@ActiveProfiles(profiles = {"mock-sam", "control-plane"})
@DirtiesContext
@SpringBootTest
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class PfbQuartzJobControlPlaneE2ETest {
  @Autowired ObjectMapper mapper;
  @Autowired CollectionService collectionService;
  @Autowired PfbTestSupport testSupport;
  @Autowired RecordSink recordSink;
  @MockBean SchedulerDao schedulerDao;
  @MockBean WorkspaceManagerDao wsmDao;

  private RawlsRecordSink rawlsRecordSink;

  @Value("classpath:pfb/minimal-data.pfb")
  Resource minimalDataPfb;

  @Value("classpath:batch-write-rawls/from-minimal-data-pfb.json")
  Resource minimalDataExpectedJson;

  @Value("classpath:pfb/data-with-array.pfb")
  Resource dataWithArrayPfb;

  @Value("classpath:batch-write-rawls/from-data-with-array-pfb.json")
  Resource dataWithArrayExpectedJson;

  private UUID collectionId;

  @BeforeAll
  void beforeAll() {
    doNothing().when(schedulerDao).schedule(any());
  }

  @BeforeEach
  void beforeEach() {
    collectionId = UUID.randomUUID();
    // TODO(AJ-1591): cWDS should not require instances to exist
    collectionService.createCollection(collectionId, "v0.2");
    rawlsRecordSink = assertInstanceOf(RawlsRecordSink.class, recordSink);
    rawlsRecordSink.getRecordedEntities().clear(); // reset the recorded entries each test
    // stub out WSM to report no snapshots already linked to this workspace
    when(wsmDao.enumerateDataRepoSnapshotReferences(any(), anyInt(), anyInt()))
        .thenReturn(new ResourceList());
  }

  @AfterEach
  void afterEach() {
    collectionService.deleteCollection(collectionId, "v0.2");
  }

  /* import test.avro, and validate the tables and row counts it imported. */
  @Test
  @Tag(SLOW)
  void pfbToRawlsEntity() throws JobExecutionException, IOException {
    testSupport.executePfbImportQuartzJob(collectionId, minimalDataPfb);
    var entities = assertRecordedEntitiesSerde(minimalDataExpectedJson);
    assertThat(entities.size()).isEqualTo(1);
    var entity = entities.get(0);
    assertThat(entity.name()).isEqualTo("HG01101_cram");
    assertThat(entity.entityType()).isEqualTo("submitted_aligned_reads");

    int totalAttributes = 19; // based on the contents of the input file
    int nullAttributes = 3; // based on the contents of the input file
    int expectedAttributes = totalAttributes - nullAttributes;
    assertThat(entity.operations().size()).isEqualTo(expectedAttributes);

    assertAttributeValue(entity, "pfb:md5sum", "bdf121aadba028d57808101cb4455fa7");
    assertAttributeValue(entity, "pfb:file_size", BigInteger.valueOf(512));
    assertAttributeValue(entity, "pfb:file_state", "registered");
    assertAttributeValue(
        entity,
        "pfb:ga4gh_drs_uri",
        "drs://example.org/dg.4503/cc32d93d-a73c-4d2c-a061-26c0410e74fa");
  }

  @Test
  @Tag(SLOW)
  void pfbToRawlsEntityWithArrays() throws JobExecutionException, IOException {
    testSupport.executePfbImportQuartzJob(collectionId, dataWithArrayPfb);
    var entities = assertRecordedEntitiesSerde(dataWithArrayExpectedJson);

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
            new AddListMember(arrayProp, "00000000-0000-0000-0000-000000000000"),
            new AddListMember(arrayProp, "11111111-1111-1111-1111-111111111111"),
            new AddListMember(arrayProp, "22222222-2222-2222-2222-222222222222"));
    assertThat(actual).isEqualTo(expected);
  }

  private void assertAttributeValue(Entity entity, String attributeName, Object expected) {
    assertThat(
            filteredOps(entity, ADD_UPDATE_ATTRIBUTE)
                .map(AddUpdateAttribute.class::cast)
                .filter(attr -> attr.attributeName().equals(attributeName))
                .map(AddUpdateAttribute::addUpdateAttribute)
                .findFirst()
                .orElseThrow())
        .isEqualTo(expected);
  }

  private Stream<? extends AttributeOperation> filteredOps(Entity entity, Op operation) {
    return entity.operations().stream().filter(op -> op.op() == operation);
  }

  // serde == serialize then deserialize; to test the full roundtrip to/from JSON
  private List<Model.Entity> assertRecordedEntitiesSerde(Resource expectedJsonResource) {
    try {
      String actualJson = mapper.writeValueAsString(rawlsRecordSink.getRecordedEntities());
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
}