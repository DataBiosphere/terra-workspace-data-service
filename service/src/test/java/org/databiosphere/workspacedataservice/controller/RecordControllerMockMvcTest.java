package org.databiosphere.workspacedataservice.controller;

import static org.assertj.core.api.Assertions.assertThat;
import static org.databiosphere.workspacedataservice.TestUtils.generateRandomAttributes;
import static org.databiosphere.workspacedataservice.service.model.ReservedNames.RECORD_ID;
import static org.hamcrest.Matchers.*;
import static org.junit.jupiter.api.Assertions.*;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.*;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.*;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import org.databiosphere.workspacedataservice.TestUtils;
import org.databiosphere.workspacedataservice.dao.TestDao;
import org.databiosphere.workspacedataservice.service.RelationUtils;
import org.databiosphere.workspacedataservice.service.model.AttributeSchema;
import org.databiosphere.workspacedataservice.service.model.DataTypeMapping;
import org.databiosphere.workspacedataservice.service.model.RecordTypeSchema;
import org.databiosphere.workspacedataservice.service.model.exception.InvalidNameException;
import org.databiosphere.workspacedataservice.service.model.exception.InvalidRelationException;
import org.databiosphere.workspacedataservice.service.model.exception.MissingObjectException;
import org.databiosphere.workspacedataservice.shared.model.BatchOperation;
import org.databiosphere.workspacedataservice.shared.model.OperationType;
import org.databiosphere.workspacedataservice.shared.model.Record;
import org.databiosphere.workspacedataservice.shared.model.RecordAttributes;
import org.databiosphere.workspacedataservice.shared.model.RecordQueryResponse;
import org.databiosphere.workspacedataservice.shared.model.RecordRequest;
import org.databiosphere.workspacedataservice.shared.model.RecordResponse;
import org.databiosphere.workspacedataservice.shared.model.RecordType;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.MediaType;
import org.springframework.mock.web.MockMultipartFile;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.web.servlet.MvcResult;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.web.method.annotation.MethodArgumentTypeMismatchException;

@ActiveProfiles(profiles = "mock-sam")
@DirtiesContext
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class RecordControllerMockMvcTest extends MockMvcTestBase {

  private UUID instanceId;

  private static final String versionId = "v0.2";

  @Autowired TestDao testDao;

  @BeforeEach
  void setUp() throws Exception {
    instanceId = UUID.randomUUID();
    mockMvc
        .perform(post("/instances/{v}/{instanceid}", versionId, instanceId).content(""))
        .andExpect(status().isCreated());
  }

  @AfterEach
  void tearDown() {
    try {
      mockMvc
          .perform(delete("/instances/{v}/{instanceid}", versionId, instanceId).content(""))
          .andExpect(status().isOk());
    } catch (Throwable t) {
      // noop - if we fail to delete the instance, don't fail the test
    }
  }

  @AfterAll
  void deleteAllInstances() throws Exception {
    MvcResult response = mockMvc.perform(get("/instances/{v}", versionId)).andReturn();
    UUID[] allInstances = fromJson(response, UUID[].class);
    for (UUID id : allInstances) {
      mockMvc.perform(delete("/instances/{v}/{instanceid}", versionId, id).content(""));
    }
  }

  @Test
  @Transactional
  void createInstanceAndTryToCreateAgain() throws Exception {
    UUID uuid = UUID.randomUUID();
    mockMvc
        .perform(post("/instances/{version}/{instanceId}", versionId, uuid))
        .andExpect(status().isCreated());
    mockMvc
        .perform(post("/instances/{version}/{instanceId}", versionId, uuid))
        .andExpect(status().isConflict());
  }

  @Test
  void listInstances() throws Exception {
    // get initial instance list
    MvcResult initialResult =
        mockMvc
            .perform(get("/instances/{version}", versionId))
            .andExpect(status().isOk())
            .andReturn();
    UUID[] initialInstances = fromJson(initialResult, UUID[].class);
    // create new uuid; new uuid should not be in our initial instance list
    UUID uuid = UUID.randomUUID();
    assertFalse(
        Arrays.asList(initialInstances).contains(uuid),
        "initial instance list should not contain brand new UUID");
    // create a new instance from the new uuid
    mockMvc
        .perform(post("/instances/{version}/{instanceId}", versionId, uuid))
        .andExpect(status().isCreated());
    // get instance list again
    MvcResult afterCreationResult =
        mockMvc
            .perform(get("/instances/{version}", versionId))
            .andExpect(status().isOk())
            .andReturn();
    UUID[] afterCreationInstances = fromJson(afterCreationResult, UUID[].class);
    // new uuid should be in our initial instance list
    assertTrue(
        Arrays.asList(afterCreationInstances).contains(uuid),
        "after-creation instance list should contain brand new UUID");

    assertEquals(
        initialInstances.length + 1,
        afterCreationInstances.length,
        "size of after-creation list should be equal to the initial size plus one");
  }

  @Test
  void createRecordTypeAndSubsequentlyTryToChangePrimaryKey() throws Exception {
    String recordType = "pk_change_test";
    createSomeRecords(recordType, 1);
    RecordAttributes attributes = generateRandomAttributes();
    RecordRequest recordRequest = new RecordRequest(attributes);
    mockMvc
        .perform(
            put(
                    "/{instanceId}/records/{version}/{recordType}/{recordId}?primaryKey={pk}",
                    instanceId,
                    versionId,
                    recordType,
                    "new_id",
                    "new_pk")
                .content(toJson(recordRequest))
                .contentType(MediaType.APPLICATION_JSON))
        .andExpect(status().isBadRequest());
  }

  @Test
  void secondBatchWriteWithPKChangeShouldFail() throws Exception {
    String newBatchRecordType = "pk-record-type-change";
    Record record =
        new Record(
            "foo1",
            RecordType.valueOf(newBatchRecordType),
            new RecordAttributes(Map.of("attr1", "attr-val")));

    BatchOperation op = new BatchOperation(record, OperationType.UPSERT);
    mockMvc
        .perform(
            post(
                    "/{instanceid}/batch/{v}/{type}?primaryKey={pk}",
                    instanceId,
                    versionId,
                    newBatchRecordType,
                    "pk1")
                .content(toJson(List.of(op)))
                .contentType(MediaType.APPLICATION_JSON))
        .andExpect(status().isOk());
    Record record2 =
        new Record(
            "foo2",
            RecordType.valueOf(newBatchRecordType),
            new RecordAttributes(Map.of("attr1", "attr-val")));
    mockMvc
        .perform(
            post(
                    "/{instanceid}/batch/{v}/{type}?=primaryKey={pk}",
                    instanceId,
                    versionId,
                    newBatchRecordType,
                    "pkUpdated")
                .content(toJson(List.of(new BatchOperation(record2, OperationType.UPSERT))))
                .contentType(MediaType.APPLICATION_JSON))
        .andExpect(status().isBadRequest());
  }

  @Test
  @Transactional
  void deleteInstance() throws Exception {
    UUID uuid = UUID.randomUUID();
    // delete nonexistent instance should 404
    mockMvc
        .perform(delete("/instances/{version}/{instanceId}", versionId, uuid))
        .andExpect(status().isNotFound());
    // creating the instance should 201
    mockMvc
        .perform(post("/instances/{version}/{instanceId}", versionId, uuid))
        .andExpect(status().isCreated());
    // delete existing instance should 200
    mockMvc
        .perform(delete("/instances/{version}/{instanceId}", versionId, uuid))
        .andExpect(status().isOk());
    // deleting again should 404
    mockMvc
        .perform(delete("/instances/{version}/{instanceId}", versionId, uuid))
        .andExpect(status().isNotFound());
    // creating again should 201
    mockMvc
        .perform(post("/instances/{version}/{instanceId}", versionId, uuid))
        .andExpect(status().isCreated());
  }

  @Test
  @Transactional
  void deleteInstanceContainingData() throws Exception {
    RecordAttributes attributes = new RecordAttributes(Map.of("foo", "bar", "num", 123));
    // create "to" record, which will be the target of a relation
    mockMvc
        .perform(
            put(
                    "/{instanceId}/records/{version}/{recordType}/{recordId}",
                    instanceId,
                    versionId,
                    "to",
                    "1")
                .content(toJson(new RecordRequest(attributes)))
                .contentType(MediaType.APPLICATION_JSON))
        .andExpect(status().isCreated());
    // create "from" record, with a relation to "to"
    RecordAttributes attributes2 = new RecordAttributes(Map.of("relation", "terra-wds:/to/1"));
    mockMvc
        .perform(
            put(
                    "/{instanceId}/records/{version}/{recordType}/{recordId}",
                    instanceId,
                    versionId,
                    "from",
                    "2")
                .content(toJson(new RecordRequest(attributes2)))
                .contentType(MediaType.APPLICATION_JSON))
        .andExpect(status().isCreated());
    // delete existing instance should 200
    mockMvc
        .perform(delete("/instances/{version}/{instanceId}", versionId, instanceId))
        .andExpect(status().isOk());
  }

  @Test
  @Transactional
  void tsvWithNoRowsShouldReturn400() throws Exception {
    MockMultipartFile file =
        new MockMultipartFile(
            "records", "no_data.tsv", MediaType.TEXT_PLAIN_VALUE, "col1\tcol2\n".getBytes());

    mockMvc
        .perform(
            multipart(
                    "/{instanceId}/tsv/{version}/{recordType}",
                    instanceId,
                    versionId,
                    "tsv-record-type")
                .file(file))
        .andExpect(status().isBadRequest());
  }

  @Test
  @Transactional
  void storeLargeIntegerValue() throws Exception {
    String bigIntValue = "11111111111111111111111111111111";
    String bigFloatValue = "11111111111111111111111111111111.2222222222";
    MockMultipartFile file =
        new MockMultipartFile(
            "records",
            "simple.tsv",
            MediaType.TEXT_PLAIN_VALUE,
            ("sys_name\tbigint\tbigfloat\n" + 1 + "\t" + bigIntValue + "\t" + bigFloatValue + "\n")
                .getBytes());

    String recordType = "big-int-value";
    mockMvc
        .perform(
            multipart("/{instanceId}/tsv/{version}/{recordType}", instanceId, versionId, recordType)
                .file(file))
        .andExpect(status().isOk());
    MvcResult mvcResult =
        mockMvc
            .perform(
                get(
                    "/{instanceId}/records/{versionId}/{recordType}/{recordId}",
                    instanceId,
                    versionId,
                    recordType,
                    "1"))
            .andReturn();
    RecordResponse recordResponse = fromJson(mvcResult, RecordResponse.class);
    assertEquals(
        new BigInteger(bigIntValue), recordResponse.recordAttributes().getAttributeValue("bigint"));
    assertEquals(
        new BigDecimal(bigFloatValue),
        recordResponse.recordAttributes().getAttributeValue("bigfloat"));
  }

  @Test
  @Transactional
  void writeAndReadJson() throws Exception {
    String rt = "jsonb-type";
    RecordAttributes attributes =
        new RecordAttributes(Map.of("json-attr", Map.of("name", "Bella", "age_in_months", 8)));
    // create new record with new record type
    String rId = "newRecordId";
    mockMvc
        .perform(
            put(
                    "/{instanceId}/records/{version}/{recordType}/{recordId}",
                    instanceId,
                    versionId,
                    rt,
                    rId)
                .content(toJson(new RecordRequest(attributes)))
                .contentType(MediaType.APPLICATION_JSON))
        .andExpect(status().isCreated());
    MvcResult result =
        mockMvc
            .perform(
                get(
                    "/{instanceId}/records/{version}/{recordType}/{recordId}",
                    instanceId,
                    versionId,
                    rt,
                    rId))
            .andExpect(jsonPath("$.attributes.json-attr.age_in_months", is(8)))
            .andReturn();
    RecordResponse recordResponse = fromJson(result, RecordResponse.class);
    Object attributeValue = recordResponse.recordAttributes().getAttributeValue("json-attr");
    assertInstanceOf(
        Map.class,
        attributeValue,
        "jsonb data should deserialize to a map, "
            + "before getting serialized to json in the final response");
  }

  @Test
  @Transactional
  void writeAndReadAllDataTypesJson() throws Exception {
    // Create target records - note that getAllTypesAttributesForJson expects relations to be
    // "target-record"
    createSomeRecords(RecordType.valueOf("target-record"), 2);
    String rt = "all-types";
    RecordAttributes attributes = TestUtils.getAllTypesAttributesForJson();
    assertEquals(attributes.attributeSet().size(), DataTypeMapping.values().length);
    String rId = "newRecordId";
    mockMvc
        .perform(
            put(
                    "/{instanceId}/records/{version}/{recordType}/{recordId}",
                    instanceId,
                    versionId,
                    rt,
                    rId)
                .content(toJson(new RecordRequest(attributes)))
                .contentType(MediaType.APPLICATION_JSON))
        .andExpect(status().isCreated());
    String jsonRes =
        mockMvc
            .perform(
                get(
                    "/{instanceId}/records/{version}/{recordType}/{recordId}",
                    instanceId,
                    versionId,
                    rt,
                    rId))
            .andReturn()
            .getResponse()
            .getContentAsString();
    assertEquals(TestUtils.getExpectedAllAttributesJsonText(), jsonRes);
  }

  @Test
  @Transactional
  void writeAndReadAllDataTypesTsv() throws Exception {
    // Create target records - note that getAllTypesAttributesForTsv expects relations to be
    // "target-record"
    createSomeRecords(RecordType.valueOf("target-record"), 2);
    String rt = "all-types";
    String recordId = "newRecordId";
    RecordAttributes attributes = TestUtils.getAllTypesAttributesForTsv();
    assertEquals(DataTypeMapping.values().length, attributes.attributeSet().size());
    String tsv =
        "sys_name\t"
            + attributes.attributeSet().stream()
                .map(Map.Entry::getKey)
                .collect(Collectors.joining("\t"))
            + "\n";
    tsv +=
        recordId
            + "\t"
            + attributes.attributeSet().stream()
                .map(e -> e.getValue().toString())
                .collect(Collectors.joining("\t"))
            + "\n";

    MockMultipartFile file =
        new MockMultipartFile(
            "records", "generated.tsv", MediaType.TEXT_PLAIN_VALUE, tsv.getBytes());
    mockMvc
        .perform(
            multipart("/{instanceId}/tsv/{version}/{recordType}", instanceId, versionId, rt)
                .file(file))
        .andExpect(status().isOk());
    String jsonRes =
        mockMvc
            .perform(
                get(
                    "/{instanceId}/records/{version}/{recordType}/{recordId}",
                    instanceId,
                    versionId,
                    rt,
                    recordId))
            .andReturn()
            .getResponse()
            .getContentAsString();
    assertEquals(TestUtils.getExpectedAllAttributesJsonText(), jsonRes);
  }

  @Test
  @Transactional
  void tsvWithMissingRowIdentifierColumn() throws Exception {
    MockMultipartFile file =
        new MockMultipartFile(
            "records",
            "no_row_id.tsv",
            MediaType.TEXT_PLAIN_VALUE,
            "col1\tcol2\nfoo\tbar\n".getBytes());

    mockMvc
        .perform(
            multipart(
                    "/{instanceId}/tsv/{version}/{recordType}?primaryKey=missing_row_id",
                    instanceId,
                    versionId,
                    "tsv-missing-rowid")
                .file(file))
        .andExpect(status().isBadRequest());
  }

  @Test
  void tsvChangePrimaryKeyShouldFail() throws Exception {
    MockMultipartFile file =
        new MockMultipartFile(
            "records",
            "tsv_pk_change.tsv",
            MediaType.TEXT_PLAIN_VALUE,
            "col1\tcol2\nfoo\tbar\n".getBytes());

    String recordType = "tsv-pk-change";
    mockMvc
        .perform(
            multipart("/{instanceId}/tsv/{version}/{recordType}", instanceId, versionId, recordType)
                .file(file))
        .andExpect(status().isOk());
    // col1 as the left most column would have been selected as PK above, now we try to flip the
    // script and this should return an error
    mockMvc
        .perform(
            multipart(
                    "/{instanceId}/tsv/{version}/{recordType}?primaryKey=col2",
                    instanceId,
                    versionId,
                    recordType)
                .file(file))
        .andExpect(status().isBadRequest());
  }

  @Test
  void tsvConflictPrimaryKeyShouldFail() throws Exception {
    MockMultipartFile file =
        new MockMultipartFile(
            "records",
            "tsv_orig.tsv",
            MediaType.TEXT_PLAIN_VALUE,
            "col1\tcol2\nfoo\tbar\n".getBytes());

    String recordType = "tsv-pk-change";
    mockMvc
        .perform(
            multipart("/{instanceId}/tsv/{version}/{recordType}", instanceId, versionId, recordType)
                .file(file))
        .andExpect(status().isOk());

    MockMultipartFile file2 =
        new MockMultipartFile(
            "records",
            "tsv_pk_change.tsv",
            MediaType.TEXT_PLAIN_VALUE,
            "id\tcol2\nfoo\tbar\n".getBytes());
    MvcResult mvcResult =
        mockMvc
            .perform(
                multipart(
                        "/{instanceId}/tsv/{version}/{recordType}",
                        instanceId,
                        versionId,
                        recordType)
                    .file(file2))
            .andExpect(status().isBadRequest())
            .andReturn();
    // Return message should be helpful
    Exception e = mvcResult.getResolvedException();
    assertNotNull(e, "expected an InvalidTsvException");
    assertTrue(e.getMessage().contains("Uploaded TSV is either missing"));
  }

  @Test
  @Transactional
  void tsvWithEmptyStringIdentifier() throws Exception {
    MockMultipartFile file =
        new MockMultipartFile(
            "records",
            "empty_row_id.tsv",
            MediaType.TEXT_PLAIN_VALUE,
            "col1\tcol2\n\tbar\n".getBytes());

    mockMvc
        .perform(
            multipart(
                    "/{instanceId}/tsv/{version}/{recordType}",
                    instanceId,
                    versionId,
                    "tsv-missing-rowid")
                .file(file))
        .andExpect(status().isBadRequest());
  }

  @Test
  @Transactional
  void tsvWithDuplicateRowIdsInSameBatch() throws Exception {
    MockMultipartFile file =
        new MockMultipartFile(
            "records",
            "duplicate_id.tsv",
            MediaType.TEXT_PLAIN_VALUE,
            """
                   idcol	col2
                   1	foo
                   2	bar
                   1	baz
                   3	qux"""
                .stripIndent()
                .getBytes());

    MvcResult mvcResult =
        mockMvc
            .perform(
                multipart(
                        "/{instanceId}/tsv/{version}/{recordType}",
                        instanceId,
                        versionId,
                        "duplicate-rowids")
                    .file(file))
            .andExpect(status().isBadRequest())
            .andReturn();

    Exception e = mvcResult.getResolvedException();
    assertNotNull(e, "expected an InvalidTsvException");
    assertEquals("TSVs cannot contain duplicate primary key values", e.getMessage());
  }

  @Test
  @Transactional
  void tsvWithDuplicateColumnNames() throws Exception {
    MockMultipartFile file =
        new MockMultipartFile(
            "records",
            "duplicate_col_name.tsv",
            MediaType.TEXT_PLAIN_VALUE,
            "col1\tcol1\nfoo\tbar\n".getBytes());

    MvcResult mvcResult =
        mockMvc
            .perform(
                multipart(
                        "/{instanceId}/tsv/{version}/{recordType}",
                        instanceId,
                        versionId,
                        "duplicate-rowids")
                    .file(file))
            .andExpect(status().isBadRequest())
            .andReturn();

    Exception e = mvcResult.getResolvedException();
    assertNotNull(e, "expected an InvalidTsvException");
    assertEquals(
        "TSV contains duplicate column names. "
            + "Please use distinct column names to prevent overwriting data",
        e.getMessage());
  }

  @Test
  @Transactional
  void tsvWithTrailingTabs() throws Exception {
    MockMultipartFile file =
        new MockMultipartFile(
            "records",
            "trailing_tabs.tsv",
            MediaType.TEXT_PLAIN_VALUE,
            "col1\tcol2\t\t\t\nfoo\tbar\n".getBytes());

    MvcResult mvcResult =
        mockMvc
            .perform(
                multipart(
                        "/{instanceId}/tsv/{version}/{recordType}",
                        instanceId,
                        versionId,
                        "duplicate-rowids")
                    .file(file))
            .andExpect(status().isBadRequest())
            .andReturn();

    Exception e = mvcResult.getResolvedException();
    assertNotNull(e, "expected an InvalidTsvException");
    assertEquals(
        "TSV headers contain unexpected whitespace. "
            + "Please delete the whitespace and resubmit.",
        e.getMessage());
  }

  @Test
  @Transactional
  void tsvWithBlankHeader() throws Exception {
    MockMultipartFile file =
        new MockMultipartFile(
            "records",
            "blank_header.tsv",
            MediaType.TEXT_PLAIN_VALUE,
            "col1\t\t\nfoo\tbar\n".getBytes());

    MvcResult mvcResult =
        mockMvc
            .perform(
                multipart(
                        "/{instanceId}/tsv/{version}/{recordType}",
                        instanceId,
                        versionId,
                        "duplicate-rowids")
                    .file(file))
            .andExpect(status().isBadRequest())
            .andReturn();

    Exception e = mvcResult.getResolvedException();
    assertNotNull(e, "expected an InvalidTsvException");
    assertEquals(
        "TSV headers contain unexpected whitespace. "
            + "Please delete the whitespace and resubmit.",
        e.getMessage());
  }

  @Test
  @Transactional
  void tsvWithNullHeader() throws Exception {
    MockMultipartFile file =
        new MockMultipartFile(
            "records",
            "null_header.tsv",
            MediaType.TEXT_PLAIN_VALUE,
            """
					value
					foo
					bar
					baz		""".getBytes());

    String recordType = "null-headers";
    mockMvc
        .perform(
            multipart("/{instanceId}/tsv/{version}/{recordType}", instanceId, versionId, recordType)
                .file(file))
        .andExpect(status().isOk());
    MvcResult schemaResult =
        mockMvc
            .perform(get("/{instanceid}/types/{v}/{type}", instanceId, versionId, recordType))
            .andReturn();
    RecordTypeSchema schema = fromJson(schemaResult, RecordTypeSchema.class);
    assertEquals(1, schema.attributes().size());
  }

  @Test
  @Transactional
  void tsvWithDuplicateRowIdsInDifferentBatches(@Value("${twds.write.batch.size}") int batchSize)
      throws Exception {
    StringBuilder tsvContent = new StringBuilder("idcol\tcol1\n");
    // append two separate batches, each of which use the same record ids
    for (int batch = 0; batch < 2; batch++) {
      for (int i = 0; i < batchSize; i++) {
        tsvContent.append(i).append("\ttada").append(batch).append("_").append(i).append("\n");
      }
    }
    MockMultipartFile file =
        new MockMultipartFile(
            "records", "simple.tsv", MediaType.TEXT_PLAIN_VALUE, tsvContent.toString().getBytes());

    MvcResult mvcResult =
        mockMvc
            .perform(
                multipart(
                        "/{instanceId}/tsv/{version}/{recordType}",
                        instanceId,
                        versionId,
                        "tsv_batching")
                    .file(file))
            .andExpect(status().isBadRequest())
            .andReturn();

    Exception e = mvcResult.getResolvedException();
    assertNotNull(e, "expected an InvalidTsvException");
    assertEquals("TSVs cannot contain duplicate primary key values", e.getMessage());
  }

  @Test
  @Transactional
  void tsvWithSpecifiedRowIdentifierColumn() throws Exception {
    MockMultipartFile file =
        new MockMultipartFile(
            "records",
            "specified_id.tsv",
            MediaType.TEXT_PLAIN_VALUE,
            "col1\tcol2\nfoo\tbar\n".getBytes());

    String recordType = "tsv_specified_row_id";
    mockMvc
        .perform(
            multipart(
                    "/{instanceId}/tsv/{version}/{recordType}?primaryKey=col2",
                    instanceId,
                    versionId,
                    recordType)
                .file(file))
        .andExpect(status().isOk());
    mockMvc
        .perform(
            get(
                "/{instanceId}/records/{version}/{recordType}/{recordId}",
                instanceId,
                versionId,
                recordType,
                "bar"))
        .andExpect(status().isOk());
  }

  @Test
  void handlePkDataSpecifiedTwice() throws Exception {
    String recordType = "over_specified_primary_key";
    RecordAttributes attributes = RecordAttributes.empty().putAttribute("attr1", "test");
    RecordRequest recordRequest = new RecordRequest(attributes);
    mockMvc
        .perform(
            put(
                    "/{instanceId}/records/{version}/{recordType}/{recordId}?primaryKey={pk}",
                    instanceId,
                    versionId,
                    recordType,
                    "new_id",
                    "attr1")
                .content(toJson(recordRequest))
                .contentType(MediaType.APPLICATION_JSON))
        .andExpect(status().isCreated());
  }

  @Test
  @Transactional
  void simpleTsvUploadWithBatchingShouldSucceed(@Value("${twds.write.batch.size}") int batchSize)
      throws Exception {
    StringBuilder tsvContent = new StringBuilder("sys_name\tcol1\n");
    for (int i = 0; i < batchSize + 1; i++) {
      tsvContent.append(i).append("\ttada").append(i).append("\n");
    }
    MockMultipartFile file =
        new MockMultipartFile(
            "records", "simple.tsv", MediaType.TEXT_PLAIN_VALUE, tsvContent.toString().getBytes());

    mockMvc
        .perform(
            multipart(
                    "/{instanceId}/tsv/{version}/{recordType}",
                    instanceId,
                    versionId,
                    "tsv_batching")
                .file(file))
        .andExpect(status().isOk());
  }

  @Test
  @Transactional
  void tsvUploadWithRelationsShouldSucceed() throws Exception {
    RecordType refType = RecordType.valueOf("refType");
    createSomeRecords(refType, 3);

    StringBuilder tsvContent = new StringBuilder("sys_name\trel\trelArr\n");
    String singleRel = RelationUtils.createRelationString(refType, "record_0");
    String relArr =
        "[\""
            + RelationUtils.createRelationString(refType, "record_1")
            + "\", \""
            + RelationUtils.createRelationString(refType, "record_2")
            + "\"]";
    for (int i = 0; i < 6; i++) {
      tsvContent.append(i).append("\t").append(singleRel).append("\t").append(relArr).append("\n");
    }
    MockMultipartFile file =
        new MockMultipartFile(
            "records",
            "relation.tsv",
            MediaType.TEXT_PLAIN_VALUE,
            tsvContent.toString().getBytes());

    RecordType tsvRelationType = RecordType.valueOf("tsv_relations");

    mockMvc
        .perform(
            multipart(
                    "/{instanceId}/tsv/{version}/{recordType}",
                    instanceId,
                    versionId,
                    tsvRelationType)
                .file(file))
        .andExpect(status().isOk());

    MvcResult result =
        mockMvc
            .perform(
                post(
                    "/{instanceId}/search/{version}/{recordType}",
                    instanceId,
                    versionId,
                    tsvRelationType))
            .andExpect(status().isOk())
            .andReturn();

    RecordQueryResponse response = fromJson(result, RecordQueryResponse.class);
    assertEquals(6, response.totalRecords());
    RecordAttributes exampleAttributes = response.records().get(0).recordAttributes();
    assertEquals(singleRel, exampleAttributes.getAttributeValue("rel"));
  }

  @Test
  @Transactional
  void nullAndNonNullArraysShouldChooseProperType() throws Exception {
    StringBuilder tsvContent = new StringBuilder("sys_name\tarray\n");
    // empty string/nulls
    for (int i = 0; i < 10; i++) {
      tsvContent.append(i).append("null\t").append("\n");
    }
    // empty array
    for (int i = 0; i < 10; i++) {
      tsvContent.append(i).append("empty\t[]\n");
    }
    // array of long
    for (int i = 0; i < 10; i++) {
      tsvContent.append(i).append("valid\t[12]\n");
    }
    MockMultipartFile file =
        new MockMultipartFile(
            "records", "simple.tsv", MediaType.TEXT_PLAIN_VALUE, tsvContent.toString().getBytes());

    String type = "tsv-record-type";
    mockMvc
        .perform(
            multipart("/{instanceId}/tsv/{version}/{recordType}", instanceId, versionId, type)
                .file(file))
        .andExpect(status().isOk());

    MvcResult mvcResult =
        mockMvc
            .perform(get("/{instanceId}/types/{v}/{type}", instanceId, versionId, type))
            .andExpect(status().isOk())
            .andReturn();

    RecordTypeSchema actual = fromJson(mvcResult, RecordTypeSchema.class);
    assertEquals("ARRAY_OF_NUMBER", actual.attributes().get(0).datatype());

    // upload a second time, this time with array of double
    StringBuilder secondUpload = new StringBuilder("sys_name\tarray\n");
    // array of long
    for (int i = 0; i < 10; i++) {
      secondUpload.append(i).append("valid\t[12.99]\n");
    }
    file =
        new MockMultipartFile(
            "records",
            "simple.tsv",
            MediaType.TEXT_PLAIN_VALUE,
            secondUpload.toString().getBytes());
    mockMvc
        .perform(
            multipart("/{instanceId}/tsv/{version}/{recordType}", instanceId, versionId, type)
                .file(file))
        .andExpect(status().isOk());
    mvcResult =
        mockMvc
            .perform(get("/{instanceId}/types/{v}/{type}", instanceId, versionId, type))
            .andExpect(status().isOk())
            .andReturn();

    actual = fromJson(mvcResult, RecordTypeSchema.class);
    assertEquals("ARRAY_OF_NUMBER", actual.attributes().get(0).datatype());
  }

  @Test
  @Transactional
  void scalarFollowedByArray() throws Exception {
    String type = "scalar-followed-by-array";
    String id = "my-id";
    String attrName = "my-attr";

    RecordAttributes firstPayload = new RecordAttributes(Map.of(attrName, "simple string"));
    RecordAttributes secondPayload =
        new RecordAttributes(Map.of(attrName, List.of("array", "of", "strings")));

    // upload the scalar string, should be success
    mockMvc
        .perform(
            put(
                    "/{instanceId}/records/{version}/{recordType}/{id}",
                    instanceId,
                    versionId,
                    type,
                    id)
                .content(toJson(new RecordRequest(firstPayload)))
                .contentType(MediaType.APPLICATION_JSON))
        .andExpect(status().is2xxSuccessful());

    MvcResult firstResult =
        mockMvc
            .perform(
                get(
                    "/{instanceId}/records/{version}/{recordType}/{id}",
                    instanceId,
                    versionId,
                    type,
                    id))
            .andReturn();
    RecordResponse firstRecord = fromJson(firstResult, RecordResponse.class);
    assertEquals("simple string", firstRecord.recordAttributes().getAttributeValue(attrName));

    // upload the string array into the same attribute, should also be success
    mockMvc
        .perform(
            put(
                    "/{instanceId}/records/{version}/{recordType}/{id}",
                    instanceId,
                    versionId,
                    type,
                    id)
                .content(toJson(new RecordRequest(secondPayload)))
                .contentType(MediaType.APPLICATION_JSON))
        .andExpect(status().is2xxSuccessful());

    MvcResult secondResult =
        mockMvc
            .perform(
                get(
                    "/{instanceId}/records/{version}/{recordType}/{id}",
                    instanceId,
                    versionId,
                    type,
                    id))
            .andReturn();
    RecordResponse secondRecord = fromJson(secondResult, RecordResponse.class);
    assertEquals("{array,of,strings}", secondRecord.recordAttributes().getAttributeValue(attrName));
  }

  @Test
  @Transactional
  void tsvWithMissingRelationShouldFail() throws Exception {
    MockMultipartFile file =
        new MockMultipartFile(
            "records",
            "simple_bad_relation.tsv",
            MediaType.TEXT_PLAIN_VALUE,
            ("sys_name\trelation\na\t"
                    + RelationUtils.createRelationString(RecordType.valueOf("missing"), "QQ")
                    + "\n")
                .getBytes());

    mockMvc
        .perform(
            multipart(
                    "/{instanceId}/tsv/{version}/{recordType}",
                    instanceId,
                    versionId,
                    "tsv-record-type")
                .file(file))
        .andExpect(status().isNotFound());
  }

  @Test
  @Transactional
  void tsvUploadUsesFirstColumnAsPrimaryKey() throws Exception {
    MockMultipartFile file =
        new MockMultipartFile(
            "records",
            "test.tsv",
            MediaType.TEXT_PLAIN_VALUE,
            RecordControllerMockMvcTest.class.getResourceAsStream("/tsv/small-no-sys.tsv"));
    String recordType = "noPrimaryKeySpecified";
    mockMvc
        .perform(
            multipart("/{instanceId}/tsv/{version}/{recordType}", instanceId, versionId, recordType)
                .file(file))
        .andExpect(status().isOk());
    MvcResult schemaResult =
        mockMvc
            .perform(get("/{instanceid}/types/{v}/{type}", instanceId, versionId, recordType))
            .andReturn();
    RecordTypeSchema schema = fromJson(schemaResult, RecordTypeSchema.class);
    // "greeting" is hardcoded into small-no-sys.tsv
    assertEquals("greeting", schema.primaryKey());
  }

  @Test
  @Transactional
  void tsvUploadUsesSpecifiedColumnAsPrimaryKey() throws Exception {
    String pk = "embedded characters";
    MockMultipartFile file =
        new MockMultipartFile(
            "records",
            "test.tsv",
            MediaType.TEXT_PLAIN_VALUE,
            RecordControllerMockMvcTest.class.getResourceAsStream("/tsv/small-no-sys.tsv"));
    String recordType = "explicitPrimaryKey";
    mockMvc
        .perform(
            multipart("/{instanceId}/tsv/{version}/{recordType}", instanceId, versionId, recordType)
                .file(file)
                .queryParam("primaryKey", pk))
        .andExpect(status().isOk());
    MvcResult schemaResult =
        mockMvc
            .perform(get("/{instanceid}/types/{v}/{type}", instanceId, versionId, recordType))
            .andReturn();
    RecordTypeSchema schema = fromJson(schemaResult, RecordTypeSchema.class);
    assertEquals(pk, schema.primaryKey());
  }

  @Test
  @Transactional
  void uploadTsvAndVerifySchema() throws Exception {
    MockMultipartFile file =
        new MockMultipartFile(
            "records",
            "test.tsv",
            MediaType.TEXT_PLAIN_VALUE,
            RecordControllerMockMvcTest.class.getResourceAsStream("/tsv/small-test.tsv"));

    String recordType = "tsv-types";
    mockMvc
        .perform(
            multipart("/{instanceId}/tsv/{version}/{recordType}", instanceId, versionId, recordType)
                .file(file))
        .andExpect(status().isOk());
    MvcResult schemaResult =
        mockMvc
            .perform(get("/{instanceid}/types/{v}/{type}", instanceId, versionId, recordType))
            .andReturn();
    RecordTypeSchema schema = fromJson(schemaResult, RecordTypeSchema.class);
    assertEquals("date", schema.attributes().get(0).name());
    assertEquals("DATE", schema.attributes().get(0).datatype());
    assertEquals("NUMBER", schema.attributes().get(1).datatype());
    assertEquals("double", schema.attributes().get(1).name());
    assertEquals("json", schema.attributes().get(4).name());
    assertEquals("JSON", schema.attributes().get(4).datatype());
    assertEquals("NUMBER", schema.attributes().get(5).datatype());
    assertEquals("long", schema.attributes().get(5).name());
    assertEquals("z_array_of_string", schema.attributes().get(8).name());
    assertEquals("ARRAY_OF_STRING", schema.attributes().get(8).datatype());
    assertEquals("z_double_array", schema.attributes().get(9).name());
    assertEquals("ARRAY_OF_NUMBER", schema.attributes().get(9).datatype());
    assertEquals("z_long_array", schema.attributes().get(10).name());
    assertEquals("ARRAY_OF_NUMBER", schema.attributes().get(10).datatype());
    assertEquals("z_z_boolean_array", schema.attributes().get(11).name());
    assertEquals("ARRAY_OF_BOOLEAN", schema.attributes().get(11).datatype());
    assertEquals("zz_array_of_date", schema.attributes().get(12).name());
    assertEquals("ARRAY_OF_DATE", schema.attributes().get(12).datatype());
    assertEquals("zz_array_of_datetime", schema.attributes().get(13).name());
    assertEquals("ARRAY_OF_DATE_TIME", schema.attributes().get(13).datatype());
    MockMultipartFile alter =
        new MockMultipartFile(
            "records",
            "change_json_to_text.tsv",
            MediaType.TEXT_PLAIN_VALUE,
            "sys_name\tjson\na\tfoo\n".getBytes());
    mockMvc
        .perform(
            multipart("/{instanceId}/tsv/{version}/{recordType}", instanceId, versionId, recordType)
                .file(alter))
        .andExpect(status().isOk());
    schemaResult =
        mockMvc
            .perform(get("/{instanceid}/types/{v}/{type}", instanceId, versionId, recordType))
            .andReturn();
    schema = fromJson(schemaResult, RecordTypeSchema.class);
    assertEquals("json", schema.attributes().get(4).name());
    // data type should downgrade to STRING
    assertEquals("STRING", schema.attributes().get(4).datatype());
    // make sure left most column (sys_name) is used as id
    mockMvc
        .perform(
            get(
                "/{instanceId}/records/{version}/{recordType}/{recordId}",
                instanceId,
                versionId,
                recordType,
                "a"))
        .andExpect(status().isOk());
  }

  @Test
  @Transactional
  void tryDeletingMissingRecordType() throws Exception {
    mockMvc
        .perform(
            delete(
                "/{instanceId}/records/{versionId}/{recordType}/{recordId}",
                instanceId,
                versionId,
                "missing",
                "missing-also"))
        .andExpect(status().isNotFound());
  }

  @Test
  @Transactional
  void tryFetchingMissingRecordType() throws Exception {
    mockMvc
        .perform(
            get(
                "/{instanceId}/records/{versionId}/{recordType}/{recordId}",
                instanceId,
                versionId,
                "missing",
                "missing-2"))
        .andExpect(status().isNotFound());
  }

  @Test
  @Transactional
  void tryFetchingMissingRecord() throws Exception {
    RecordType recordType1 = RecordType.valueOf("recordType1");
    createSomeRecords(recordType1, 1);
    mockMvc
        .perform(
            get(
                "/{instanceId}/records/{versionId}/{recordType}/{recordId}",
                instanceId,
                versionId,
                recordType1,
                "missing-2"))
        .andExpect(status().isNotFound());
  }

  @Test
  @Transactional
  void tryCreatingIllegallyNamedRecordType() throws Exception {
    String recordType = "sys_my_type";
    RecordAttributes attributes = RecordAttributes.empty();
    mockMvc
        .perform(
            put(
                    "/{instanceId}/records/{version}/{recordType}/{recordId}",
                    instanceId,
                    versionId,
                    recordType,
                    "recordId")
                .content(toJson(new RecordRequest(attributes)))
                .contentType(MediaType.APPLICATION_JSON))
        .andExpect(status().isBadRequest())
        .andExpect(
            result ->
                assertInstanceOf(
                    MethodArgumentTypeMismatchException.class, result.getResolvedException()));
  }

  @Test
  @Transactional
  void updateWithIllegalAttributeName() throws Exception {
    RecordType recordType1 = RecordType.valueOf("illegalName");
    createSomeRecords(recordType1, 1);
    RecordAttributes illegalAttribute = RecordAttributes.empty();
    illegalAttribute.putAttribute("sys_foo", "some_val");
    mockMvc
        .perform(
            patch(
                    "/{instanceId}/records/{versionId}/{recordType}/{recordId}",
                    instanceId,
                    versionId,
                    recordType1,
                    "record_0")
                .contentType(MediaType.APPLICATION_JSON)
                .content(toJson(new RecordRequest(illegalAttribute))))
        .andExpect(status().isBadRequest())
        .andExpect(
            result -> assertInstanceOf(InvalidNameException.class, result.getResolvedException()));
  }

  @Test
  @Transactional
  void putNewRecord() throws Exception {
    String newRecordType = "newRecordType";
    RecordAttributes attributes = new RecordAttributes(Map.of("foo", "bar", "num", 123));
    // create new record with new record type
    mockMvc
        .perform(
            put(
                    "/{instanceId}/records/{version}/{recordType}/{recordId}",
                    instanceId,
                    versionId,
                    newRecordType,
                    "newRecordId")
                .content(toJson(new RecordRequest(attributes)))
                .contentType(MediaType.APPLICATION_JSON))
        .andExpect(status().isCreated());

    RecordAttributes attributes2 = new RecordAttributes(Map.of("foo", "baz", "num", 888));
    mockMvc
        .perform(
            put(
                    "/{instanceId}/records/{version}/{recordType}/{recordId}",
                    instanceId,
                    versionId,
                    newRecordType,
                    "newRecordId2")
                .content(toJson(new RecordRequest(attributes2)))
                .contentType(MediaType.APPLICATION_JSON))
        .andExpect(status().isCreated());

    // now update the second new record
    RecordAttributes attributes3 = new RecordAttributes(Map.of("foo", "updated", "num", 999));
    mockMvc
        .perform(
            put(
                    "/{instanceId}/records/{version}/{recordType}/{recordId}",
                    instanceId,
                    versionId,
                    newRecordType,
                    "newRecordId2")
                .content(toJson(new RecordRequest(attributes3)))
                .contentType(MediaType.APPLICATION_JSON))
        .andExpect(status().isOk());
  }

  @Test
  @Transactional
  void ensurePutShowsNewlyNullFields() throws Exception {
    RecordType recordType1 = RecordType.valueOf("recordType1");
    createSomeRecords(recordType1, 1);
    RecordAttributes newAttributes = RecordAttributes.empty();
    newAttributes.putAttribute("new-attr", "some_val");
    mockMvc
        .perform(
            put(
                    "/{instanceId}/records/{versionId}/{recordType}/{recordId}",
                    instanceId,
                    versionId,
                    recordType1,
                    "record_0")
                .contentType(MediaType.APPLICATION_JSON)
                .content(toJson(new RecordRequest(newAttributes))))
        .andExpect(content().string(containsString("\"attr3\":null")))
        .andExpect(content().string(containsString("\"attr-dt\":null")))
        .andExpect(status().isOk());
  }

  @Test
  @Transactional
  void ensurePatchShowsAllFields() throws Exception {
    RecordType recordType1 = RecordType.valueOf("recordType1");
    createSomeRecords(recordType1, 1);
    RecordAttributes newAttributes = RecordAttributes.empty();
    newAttributes.putAttribute("new-attr", "some_val");
    mockMvc
        .perform(
            patch(
                    "/{instanceId}/records/{versionId}/{recordType}/{recordId}",
                    instanceId,
                    versionId,
                    recordType1,
                    "record_0")
                .contentType(MediaType.APPLICATION_JSON)
                .content(toJson(new RecordRequest(newAttributes))))
        .andExpect(status().isOk())
        .andExpect(jsonPath("$.attributes.new-attr", is("some_val")));
  }

  @Test
  @Transactional
  void createAndRetrieveRecord() throws Exception {
    RecordType recordType = RecordType.valueOf("samples");
    createSomeRecords(recordType, 1);
    MvcResult result =
        mockMvc
            .perform(
                get(
                    "/{instanceId}/records/{version}/{recordType}/{recordId}",
                    instanceId,
                    versionId,
                    recordType,
                    "record_0"))
            .andExpect(status().isOk())
            .andReturn();
    RecordResponse recordResponse = fromJson(result, RecordResponse.class);
    assertEquals("record_0", recordResponse.recordId());
    assertEquals(
        "[1776-07-04, 1999-12-31]",
        recordResponse.recordAttributes().getAttributeValue("array-of-date").toString());
    assertEquals(
        "[2021-01-06T13:30:00, 1980-10-31T23:59:00]",
        recordResponse.recordAttributes().getAttributeValue("array-of-datetime").toString());
  }

  @Test
  @Transactional
  void createRecordWithReferences() throws Exception {
    RecordType referencedType = RecordType.valueOf("ref_participants");
    RecordType referringType = RecordType.valueOf("ref_samples");
    createSomeRecords(referencedType, 3);
    createSomeRecords(referringType, 1);
    RecordAttributes attributes = RecordAttributes.empty();
    String ref = RelationUtils.createRelationString(referencedType, "record_0");
    attributes.putAttribute("sample-ref", ref);
    mockMvc
        .perform(
            patch(
                    "/{instanceId}/records/{version}/{recordType}/{recordId}",
                    instanceId,
                    versionId,
                    referringType,
                    "record_0")
                .contentType(MediaType.APPLICATION_JSON)
                .content(toJson(new RecordRequest(attributes))))
        .andExpect(status().isOk())
        .andExpect(jsonPath("$.attributes.sample-ref", is(ref)));
  }

  @Test
  @Transactional
  void createRecordWithReferenceArray() throws Exception {
    RecordType referencedType = RecordType.valueOf("ref_participants");
    RecordType referringType = RecordType.valueOf("ref_samples");
    createSomeRecords(referencedType, 3);
    RecordAttributes attributes = RecordAttributes.empty();
    List<String> relArr =
        IntStream.range(0, 3)
            .mapToObj(Integer::toString)
            .map(i -> RelationUtils.createRelationString(referencedType, "record_" + i))
            .collect(Collectors.toList());
    attributes.putAttribute("rel-arr", relArr);
    mockMvc
        .perform(
            put(
                    "/{instanceId}/records/{version}/{recordType}/{recordId}",
                    instanceId,
                    versionId,
                    referringType,
                    "record_0")
                .contentType(MediaType.APPLICATION_JSON)
                .content(toJson(new RecordRequest(attributes))))
        .andExpect(status().isCreated())
        .andExpect(jsonPath("$.attributes.rel-arr", is(relArr)));
  }

  @Test
  @Transactional
  void addReferenceArrayColumnToExistingType() throws Exception {
    RecordType referencedType = RecordType.valueOf("ref_participants");
    RecordType referringType = RecordType.valueOf("ref_samples");
    createSomeRecords(referencedType, 3);
    mockMvc
        .perform(
            put(
                    "/{instanceId}/records/{version}/{recordType}/{recordId}",
                    instanceId,
                    versionId,
                    referringType,
                    "record_0")
                .contentType(MediaType.APPLICATION_JSON)
                .content(toJson(new RecordRequest(RecordAttributes.empty()))))
        .andExpect(status().isCreated());
    RecordAttributes attributes = RecordAttributes.empty();
    List<String> relArr =
        IntStream.range(0, 3)
            .mapToObj(Integer::toString)
            .map(i -> RelationUtils.createRelationString(referencedType, "record_" + i))
            .collect(Collectors.toList());
    attributes.putAttribute("rel-arr", relArr);
    mockMvc
        .perform(
            put(
                    "/{instanceId}/records/{version}/{recordType}/{recordId}",
                    instanceId,
                    versionId,
                    referringType,
                    "record_1")
                .contentType(MediaType.APPLICATION_JSON)
                .content(toJson(new RecordRequest(attributes))))
        .andExpect(status().isCreated())
        .andExpect(jsonPath("$.attributes.rel-arr", is(relArr)));
  }

  @Test
  @Transactional
  void createRecordWithReferenceArrayMissingTable() throws Exception {
    RecordType referencedType = RecordType.valueOf("ref_participants");
    RecordType referringType = RecordType.valueOf("ref_samples");
    RecordAttributes attributes = RecordAttributes.empty();
    List<String> relArr =
        IntStream.range(0, 3)
            .mapToObj(Integer::toString)
            .map(i -> RelationUtils.createRelationString(referencedType, "record_" + i))
            .collect(Collectors.toList());
    attributes.putAttribute("rel-arr", relArr);

    // Expect failure if relation table doesn't exist
    mockMvc
        .perform(
            put(
                    "/{instanceId}/records/{version}/{recordType}/{recordId}",
                    instanceId,
                    versionId,
                    referringType,
                    "record_0")
                .contentType(MediaType.APPLICATION_JSON)
                .content(toJson(new RecordRequest(attributes))))
        .andExpect(status().isNotFound());
  }

  @Test
  @Transactional
  void createRecordWithReferenceArrayMissingRecord() throws Exception {
    RecordType referencedType = RecordType.valueOf("ref_participants");
    RecordType referringType = RecordType.valueOf("ref_samples");
    RecordAttributes attributes = RecordAttributes.empty();
    List<String> relArr =
        IntStream.range(0, 3)
            .mapToObj(Integer::toString)
            .map(i -> RelationUtils.createRelationString(referencedType, "record_" + i))
            .collect(Collectors.toList());
    attributes.putAttribute("rel-arr", relArr);
    createSomeRecords(referencedType, 2);
    // Expect failure if only one relation is missing
    mockMvc
        .perform(
            put(
                    "/{instanceId}/records/{version}/{recordType}/{recordId}",
                    instanceId,
                    versionId,
                    referringType,
                    "record_0")
                .contentType(MediaType.APPLICATION_JSON)
                .content(toJson(new RecordRequest(attributes))))
        .andExpect(status().isForbidden());
  }

  @Test
  @Transactional
  void createRecordWithMixedReferenceArray() throws Exception {
    RecordType referencedType = RecordType.valueOf("ref_participants");
    RecordType referringType = RecordType.valueOf("ref_samples");
    RecordAttributes attributes = RecordAttributes.empty();
    List<String> relArr =
        IntStream.range(0, 3)
            .mapToObj(Integer::toString)
            .map(i -> RelationUtils.createRelationString(referencedType, "record_" + i))
            .collect(Collectors.toList());
    attributes.putAttribute("rel-arr", relArr);
    createSomeRecords(referencedType, 2);
    //		//Expect failure if one relation refers to a different table
    relArr.set(
        2, RelationUtils.createRelationString(RecordType.valueOf("nonExistentType"), "record_0"));
    attributes.putAttribute("rel-arr", relArr);
    mockMvc
        .perform(
            put(
                    "/{instanceId}/records/{version}/{recordType}/{recordId}",
                    instanceId,
                    versionId,
                    referringType,
                    "record_0")
                .contentType(MediaType.APPLICATION_JSON)
                .content(toJson(new RecordRequest(attributes))))
        .andExpect(status().isForbidden());
  }

  @Test
  @Transactional
  void referencingMissingTableFails() throws Exception {
    RecordType referencedType = RecordType.valueOf("missing");
    RecordType referringType = RecordType.valueOf("ref_samples-2");
    createSomeRecords(referringType, 1);
    RecordAttributes attributes = RecordAttributes.empty();
    String ref = RelationUtils.createRelationString(referencedType, "record_0");
    attributes.putAttribute("sample-ref", ref);
    mockMvc
        .perform(
            put(
                    "/{instanceId}/records/{version}/{recordType}/{recordId}",
                    instanceId,
                    versionId,
                    referringType,
                    "record_0")
                .contentType(MediaType.APPLICATION_JSON)
                .content(toJson(new RecordRequest(attributes))))
        .andExpect(status().isNotFound())
        .andExpect(
            result ->
                assertInstanceOf(MissingObjectException.class, result.getResolvedException()));
  }

  @Test
  @Transactional
  void referencingMissingRecordFails() throws Exception {
    RecordType referencedType = RecordType.valueOf("ref_participants-2");
    RecordType referringType = RecordType.valueOf("ref_samples-3");
    createSomeRecords(referencedType, 3);
    createSomeRecords(referringType, 1);
    RecordAttributes attributes = RecordAttributes.empty();
    String ref = RelationUtils.createRelationString(referencedType, "record_99");
    attributes.putAttribute("sample-ref", ref);
    mockMvc
        .perform(
            put(
                    "/{instanceId}/records/{version}/{recordType}/{recordId}",
                    instanceId,
                    versionId,
                    referringType,
                    "record_0")
                .contentType(MediaType.APPLICATION_JSON)
                .content(toJson(new RecordRequest(attributes))))
        .andExpect(status().isForbidden())
        .andExpect(
            result ->
                assertInstanceOf(InvalidRelationException.class, result.getResolvedException()));
  }

  @Test
  @Transactional
  void expandColumnDefForNewData() throws Exception {
    RecordType recordType = RecordType.valueOf("to-alter");
    createSomeRecords(recordType, 1);
    RecordAttributes attributes = RecordAttributes.empty();
    String newTextValue = "convert this column from date to text";
    attributes.putAttribute("attr3", newTextValue);
    mockMvc
        .perform(
            put(
                    "/{instanceId}/records/{version}/{recordType}/{recordId}",
                    instanceId,
                    versionId,
                    recordType,
                    "record_1")
                .contentType(MediaType.APPLICATION_JSON)
                .content(toJson(new RecordRequest(attributes))))
        .andExpect(status().isCreated())
        .andExpect(jsonPath("$.attributes.attr3", is(newTextValue)));
  }

  @Test
  @Transactional
  void patchMissingRecord() throws Exception {
    RecordType recordType = RecordType.valueOf("to-patch");
    createSomeRecords(recordType, 1);
    RecordAttributes attributes = RecordAttributes.empty();
    attributes.putAttribute("attr-boolean", true);
    String recordId = "record_missing";
    mockMvc
        .perform(
            patch(
                    "/{instanceId}/records/{version}/{recordType}/{recordId}",
                    instanceId,
                    versionId,
                    recordType,
                    recordId)
                .content(toJson(new RecordRequest(attributes)))
                .contentType(MediaType.APPLICATION_JSON))
        .andExpect(status().isNotFound());
  }

  @Test
  @Transactional
  void putRecordWithMissingTableReference() throws Exception {
    String recordType = "record-type-missing-table-ref";
    String recordId = "record_0";
    RecordAttributes attributes = RecordAttributes.empty();
    String ref = RelationUtils.createRelationString(RecordType.valueOf("missing"), "missing_also");
    attributes.putAttribute("sample-ref", ref);

    mockMvc
        .perform(
            put(
                    "/{instanceId}/records/{version}/{recordType}/{recordId}",
                    instanceId,
                    versionId,
                    recordType,
                    recordId)
                .content(toJson(new RecordRequest(attributes)))
                .contentType(MediaType.APPLICATION_JSON))
        .andExpect(status().isNotFound())
        .andExpect(
            result ->
                assertInstanceOf(MissingObjectException.class, result.getResolvedException()));
  }

  @Test
  @Transactional
  void putRecordWithMismatchedReference() throws Exception {
    RecordType referencedType = RecordType.valueOf("referenced_Type");
    RecordType referringType = RecordType.valueOf("referring_Type");
    String recordId = "record_0";
    createSomeRecords(referencedType, 1);
    createSomeRecords(referringType, 1);
    RecordAttributes attributes = RecordAttributes.empty();
    String ref = RelationUtils.createRelationString(referencedType, recordId);
    attributes.putAttribute("ref-attr", ref);
    // Add referencing attribute to referring_Type
    mockMvc
        .perform(
            patch(
                    "/{instanceId}/records/{version}/{recordType}/{recordId}",
                    instanceId,
                    versionId,
                    referringType,
                    recordId)
                .content(toJson(new RecordRequest(attributes)))
                .contentType(MediaType.APPLICATION_JSON))
        .andExpect(status().isOk());
    // Create a new referring_Type that puts a reference to a non-existent
    // recordType in the pre-existing referencing attribute
    RecordAttributes new_attributes = RecordAttributes.empty();
    String invalid_ref =
        RelationUtils.createRelationString(RecordType.valueOf("missing"), recordId);
    new_attributes.putAttribute("ref-attr", invalid_ref);

    mockMvc
        .perform(
            put(
                    "/{instanceId}/records/{version}/{recordType}/{recordId}",
                    instanceId,
                    versionId,
                    referringType,
                    "new_record")
                .content(toJson(new RecordRequest(new_attributes)))
                .contentType(MediaType.APPLICATION_JSON))
        .andExpect(status().isBadRequest());
  }

  @Test
  @Transactional
  void tryToAssignReferenceToNonRefColumn() throws Exception {
    RecordType recordType = RecordType.valueOf("ref-alter");
    createSomeRecords(recordType, 1);
    RecordAttributes attributes = RecordAttributes.empty();
    String ref = RelationUtils.createRelationString(RecordType.valueOf("missing"), "missing_also");
    attributes.putAttribute("attr1", ref);
    mockMvc
        .perform(
            patch(
                    "/{instanceId}/records/{version}/{recordType}/{recordId}",
                    instanceId,
                    versionId,
                    recordType,
                    "record_0")
                .content(toJson(new RecordRequest(attributes)))
                .contentType(MediaType.APPLICATION_JSON))
        .andExpect(status().isForbidden())
        .andExpect(
            result ->
                assertTrue(
                    result
                        .getResolvedException()
                        .getMessage()
                        .contains(
                            "relation to an existing attribute that was not configured for relations")));
  }

  @Test
  @Transactional
  void deleteRecord() throws Exception {
    RecordType recordType = RecordType.valueOf("samples");
    createSomeRecords(recordType, 1);
    mockMvc
        .perform(
            delete(
                "/{instanceId}/records/{version}/{recordType}/{recordId}",
                instanceId,
                versionId,
                recordType,
                "record_0"))
        .andExpect(status().isNoContent());
    mockMvc
        .perform(
            get(
                "/{instanceId}/records/{versionId}/{recordType}/{recordId}",
                instanceId,
                versionId,
                recordType,
                "missing-2"))
        .andExpect(status().isNotFound());
  }

  @Test
  @Transactional
  void deleteMissingRecord() throws Exception {
    RecordType recordType = RecordType.valueOf("samples");
    createSomeRecords(recordType, 1);
    mockMvc
        .perform(
            delete(
                "/{instanceId}/records/{version}/{recordType}/{recordId}",
                instanceId,
                versionId,
                recordType,
                "record_1"))
        .andExpect(status().isNotFound());
  }

  @Test
  @Transactional
  void deleteReferencedRecord() throws Exception {
    RecordType referencedType = RecordType.valueOf("ref_participants");
    RecordType referringType = RecordType.valueOf("ref_samples");
    createSomeRecords(referencedType, 1);
    createSomeRecords(referringType, 1);
    RecordAttributes attributes = RecordAttributes.empty();
    String ref = RelationUtils.createRelationString(referencedType, "record_0");
    attributes.putAttribute("sample-ref", ref);
    mockMvc
        .perform(
            patch(
                    "/{instanceId}/records/{version}/{recordType}/{recordId}",
                    instanceId,
                    versionId,
                    referringType,
                    "record_0")
                .contentType(MediaType.APPLICATION_JSON)
                .content(toJson(new RecordRequest(attributes))))
        .andExpect(status().isOk())
        .andExpect(content().string(containsString(ref)));
    mockMvc
        .perform(
            delete(
                "/{instanceId}/records/{version}/{recordType}/{recordId}",
                instanceId,
                versionId,
                referencedType,
                "record_0"))
        .andExpect(status().isBadRequest());
  }

  @Test
  @Transactional
  void deleteRecordWithRelationArray() throws Exception {
    RecordType referencedType = RecordType.valueOf("ref_participants");
    RecordType referringType = RecordType.valueOf("ref_samples");
    createSomeRecords(referencedType, 3);
    RecordAttributes attributes = RecordAttributes.empty();
    List<String> relArr =
        IntStream.range(0, 3)
            .mapToObj(Integer::toString)
            .map(i -> RelationUtils.createRelationString(referencedType, "record_" + i))
            .collect(Collectors.toList());
    attributes.putAttribute("rel-arr", relArr);
    mockMvc
        .perform(
            put(
                    "/{instanceId}/records/{version}/{recordType}/{recordId}",
                    instanceId,
                    versionId,
                    referringType,
                    "record_0")
                .contentType(MediaType.APPLICATION_JSON)
                .content(toJson(new RecordRequest(attributes))))
        .andExpect(status().isCreated())
        .andExpect(jsonPath("$.attributes.rel-arr", is(relArr)));

    mockMvc
        .perform(
            delete(
                "/{instanceId}/records/{version}/{recordType}/{recordId}",
                instanceId,
                versionId,
                referringType,
                "record_0"))
        .andExpect(status().isNoContent());
    mockMvc
        .perform(
            get(
                "/{instanceId}/records/{versionId}/{recordType}/{recordId}",
                instanceId,
                versionId,
                referringType,
                "missing-2"))
        .andExpect(status().isNotFound());
  }

  @Test
  @Transactional
  void deleteRecordType() throws Exception {
    String recordType = "recordType";
    createSomeRecords(recordType, 3);
    mockMvc
        .perform(delete("/{instanceId}/types/{v}/{type}", instanceId, versionId, recordType))
        .andExpect(status().isNoContent());
    mockMvc
        .perform(get("/{instanceId}/types/{version}/{type}", instanceId, versionId, recordType))
        .andExpect(status().isNotFound());
  }

  @Test
  @Transactional
  void deleteNonExistentRecordType() throws Exception {
    String recordType = "recordType";
    mockMvc
        .perform(
            delete("/{instanceId}/types/{version}/{recordType}", instanceId, versionId, recordType))
        .andExpect(status().isNotFound());
  }

  @Test
  @Transactional
  void deleteReferencedRecordType() throws Exception {
    RecordType referencedType = RecordType.valueOf("ref_participants");
    RecordType referringType = RecordType.valueOf("ref_samples");
    createSomeRecords(referencedType, 3);
    createSomeRecords(referringType, 1);
    RecordAttributes attributes = RecordAttributes.empty();
    String ref = RelationUtils.createRelationString(referencedType, "record_0");
    attributes.putAttribute("sample-ref", ref);
    mockMvc
        .perform(
            patch(
                    "/{instanceId}/records/{version}/{recordType}/{recordId}",
                    instanceId,
                    versionId,
                    referringType,
                    "record_0")
                .contentType(MediaType.APPLICATION_JSON)
                .content(toJson(new RecordRequest(attributes))))
        .andExpect(status().isOk())
        .andExpect(content().string(containsString(ref)));

    mockMvc
        .perform(
            delete(
                "/{instanceId}/types/{version}/{recordType}",
                instanceId,
                versionId,
                referencedType))
        .andExpect(status().isConflict());
  }

  @Test
  @Transactional
  void deleteRecordTypeWithRelationArray() throws Exception {
    RecordType referencedType = RecordType.valueOf("ref_participants");
    RecordType referringType = RecordType.valueOf("ref_samples");
    createSomeRecords(referencedType, 3);
    RecordAttributes attributes = RecordAttributes.empty();
    List<String> relArr =
        IntStream.range(0, 3)
            .mapToObj(Integer::toString)
            .map(i -> RelationUtils.createRelationString(referencedType, "record_" + i))
            .collect(Collectors.toList());
    attributes.putAttribute("rel-arr", relArr);
    mockMvc
        .perform(
            put(
                    "/{instanceId}/records/{version}/{recordType}/{recordId}",
                    instanceId,
                    versionId,
                    referringType,
                    "record_0")
                .contentType(MediaType.APPLICATION_JSON)
                .content(toJson(new RecordRequest(attributes))))
        .andExpect(status().isCreated())
        .andExpect(jsonPath("$.attributes.rel-arr", is(relArr)));

    mockMvc
        .perform(delete("/{instanceId}/types/{v}/{type}", instanceId, versionId, referringType))
        .andExpect(status().isNoContent());
    mockMvc
        .perform(get("/{instanceId}/types/{version}/{type}", instanceId, versionId, referringType))
        .andExpect(status().isNotFound());
  }

  @Test
  @Transactional
  void deleteReferencedRecordTypeWithNoRecords() throws Exception {
    RecordType referencedType = RecordType.valueOf("ref_participants");
    RecordType referringType = RecordType.valueOf("ref_samples");
    createSomeRecords(referencedType, 3);
    createSomeRecords(referringType, 1);
    RecordAttributes attributes = RecordAttributes.empty();
    String ref = RelationUtils.createRelationString(referencedType, "record_0");
    attributes.putAttribute("sample-ref", ref);
    // Create relation column
    mockMvc
        .perform(
            patch(
                    "/{instanceId}/records/{version}/{recordType}/{recordId}",
                    instanceId,
                    versionId,
                    referringType,
                    "record_0")
                .contentType(MediaType.APPLICATION_JSON)
                .content(toJson(new RecordRequest(attributes))))
        .andExpect(status().isOk())
        .andExpect(content().string(containsString(ref)));

    // Delete record from referencing type
    mockMvc
        .perform(
            delete(
                "/{instanceId}/records/{version}/{recordType}/{recordId}",
                instanceId,
                versionId,
                referringType,
                "record_0"))
        .andExpect(status().isNoContent());

    // Attempt to delete referenced type
    mockMvc
        .perform(
            delete(
                "/{instanceId}/types/{version}/{recordType}",
                instanceId,
                versionId,
                referencedType))
        .andExpect(status().isConflict());
  }

  @Test
  @Transactional
  void updateAttributeNoUpdate() throws Exception {
    String recordType = "recordType";
    createSomeRecords(recordType, 3);

    mockMvc
        .perform(
            patch(
                    "/{instanceId}/types/{v}/{type}/{attribute}",
                    instanceId,
                    versionId,
                    recordType,
                    "attr1")
                .contentType(MediaType.APPLICATION_JSON)
                .content("{}"))
        .andExpect(status().isBadRequest());
  }

  @Test
  @Transactional
  void renameAttribute() throws Exception {
    String recordType = "recordType";
    createSomeRecords(recordType, 3);
    String attributeToRename = "attr1";
    String newAttributeName = "newAttr";

    mockMvc
        .perform(
            patch(
                    "/{instanceId}/types/{v}/{type}/{attribute}",
                    instanceId,
                    versionId,
                    recordType,
                    attributeToRename)
                .contentType(MediaType.APPLICATION_JSON)
                .content(toJson(new AttributeSchema(newAttributeName))))
        .andExpect(status().isOk());

    MvcResult mvcResult =
        mockMvc
            .perform(
                get(
                    "/{instanceId}/types/{version}/{recordType}",
                    instanceId,
                    versionId,
                    recordType))
            .andReturn();

    RecordTypeSchema actual = fromJson(mvcResult, RecordTypeSchema.class);

    Set<String> attributeNames =
        actual.attributes().stream().map(AttributeSchema::name).collect(Collectors.toSet());

    assertThat(attributeNames).doesNotContain(attributeToRename).contains(newAttributeName);
  }

  @Test
  @Transactional
  void renameNonExistentAttribute() throws Exception {
    String recordType = "recordType";
    createSomeRecords(recordType, 3);

    mockMvc
        .perform(
            patch(
                    "/{instanceId}/types/{v}/{type}/{attribute}",
                    instanceId,
                    versionId,
                    recordType,
                    "doesNotExist")
                .contentType(MediaType.APPLICATION_JSON)
                .content(toJson(new AttributeSchema("newAttr"))))
        .andExpect(status().isNotFound());
  }

  @Test
  @Transactional
  void renamePrimaryKeyAttribute() throws Exception {
    String recordType = "recordType";
    createSomeRecords(recordType, 3);
    // sys_name is the default name for the primary key column used by createSomeRecords.
    String attributeToRename = "sys_name";

    mockMvc
        .perform(
            patch(
                    "/{instanceId}/types/{v}/{type}/{attribute}",
                    instanceId,
                    versionId,
                    recordType,
                    attributeToRename)
                .contentType(MediaType.APPLICATION_JSON)
                .content(toJson(new AttributeSchema("newAttr"))))
        .andExpect(status().isBadRequest());
  }

  @Test
  @Transactional
  void renameAttributeConflict() throws Exception {
    String recordType = "recordType";
    createSomeRecords(recordType, 3);
    // createSomeRecords creates records with attributes including "attr1" and "attr2".
    String attributeToRename = "attr1";
    String newAttributeName = "attr2";

    mockMvc
        .perform(
            patch(
                    "/{instanceId}/types/{v}/{type}/{attribute}",
                    instanceId,
                    versionId,
                    recordType,
                    attributeToRename)
                .contentType(MediaType.APPLICATION_JSON)
                .content(toJson(new AttributeSchema(newAttributeName))))
        .andExpect(status().isConflict());
  }

  @Test
  @Transactional
  void updateAttributeDataType() throws Exception {
    // Arrange
    String recordType = "recordType";
    createSomeRecords(recordType, 3);
    // createSomeRecords puts floats in attr2.
    String attributeToUpdate = "attr2";

    MvcResult initialGetSchemaResult =
        mockMvc
            .perform(get("/{instanceId}/types/{v}/{type}", instanceId, versionId, recordType))
            .andReturn();
    RecordTypeSchema initialRecordTypeSchema =
        fromJson(initialGetSchemaResult, RecordTypeSchema.class);
    AttributeSchema initialAttributeSchema =
        initialRecordTypeSchema.getAttributeSchema(attributeToUpdate);
    assertEquals("NUMBER", initialAttributeSchema.datatype());

    // Act
    MvcResult updateAttributeDataTypeResult =
        mockMvc
            .perform(
                patch(
                        "/{instanceId}/types/{v}/{type}/{attribute}",
                        instanceId,
                        versionId,
                        recordType,
                        attributeToUpdate)
                    .contentType(MediaType.APPLICATION_JSON)
                    .content(toJson(new AttributeSchema(null, "STRING"))))
            .andExpect(status().isOk())
            .andReturn();

    // Assert
    AttributeSchema updatedAttributeSchema =
        fromJson(updateAttributeDataTypeResult, AttributeSchema.class);
    assertEquals("STRING", updatedAttributeSchema.datatype());

    MvcResult finalGetSchemaResult =
        mockMvc
            .perform(get("/{instanceId}/types/{v}/{type}", instanceId, versionId, recordType))
            .andReturn();
    RecordTypeSchema finalRecordTypeSchema = fromJson(finalGetSchemaResult, RecordTypeSchema.class);
    AttributeSchema finalAttributeSchema =
        finalRecordTypeSchema.getAttributeSchema(attributeToUpdate);
    assertEquals("STRING", finalAttributeSchema.datatype());
  }

  @Test
  @Transactional
  void updateAttributeDataTypePrimaryKey() throws Exception {
    String recordType = "recordType";
    createSomeRecords(recordType, 3);
    // sys_name is the default name for the primary key column used by createSomeRecords.
    String attributeToUpdate = "sys_name";

    mockMvc
        .perform(
            patch(
                    "/{instanceId}/types/{v}/{type}/{attribute}",
                    instanceId,
                    versionId,
                    recordType,
                    attributeToUpdate)
                .contentType(MediaType.APPLICATION_JSON)
                .content(toJson(new AttributeSchema(null, "NUMBER", null))))
        .andExpect(status().isBadRequest());
  }

  @Test
  @Transactional
  void updateAttributeDataTypeInvalidDataType() throws Exception {
    String recordType = "recordType";
    createSomeRecords(recordType, 3);
    // createSomeRecords creates records with attributes including "attr1".
    String attributeToUpdate = "attr1";

    mockMvc
        .perform(
            patch(
                    "/{instanceId}/types/{v}/{type}/{attribute}",
                    instanceId,
                    versionId,
                    recordType,
                    attributeToUpdate)
                .contentType(MediaType.APPLICATION_JSON)
                .content(toJson(new AttributeSchema(null, "INVALID_DATA_TYPE", null))))
        .andExpect(status().isBadRequest());
  }

  @Test
  @Transactional
  void deleteAttribute() throws Exception {
    String recordType = "recordType";
    createSomeRecords(recordType, 3);
    String attributeToDelete = "attr1";
    mockMvc
        .perform(
            delete(
                "/{instanceId}/types/{v}/{type}/{attribute}",
                instanceId,
                versionId,
                recordType,
                attributeToDelete))
        .andExpect(status().isNoContent());

    mockMvc
        .perform(
            get("/{instanceId}/types/{version}/{recordType}", instanceId, versionId, recordType))
        .andExpect(content().string(not(containsString(attributeToDelete))));
  }

  @Test
  @Transactional
  void deleteNonExistentAttribute() throws Exception {
    String recordType = "recordType";
    createSomeRecords(recordType, 3);
    mockMvc
        .perform(
            delete(
                "/{instanceId}/types/{v}/{type}/{attribute}",
                instanceId,
                versionId,
                recordType,
                "doesnotexist"))
        .andExpect(status().isNotFound());
  }

  @Test
  @Transactional
  void deletePrimaryKeyAttribute() throws Exception {
    String recordType = "recordType";
    createSomeRecords(recordType, 3);
    // sys_name is the default name for the primary key column used by createSomeRecords.
    String attributeToDelete = "sys_name";

    mockMvc
        .perform(
            delete(
                "/{instanceId}/types/{v}/{type}/{attribute}",
                instanceId,
                versionId,
                recordType,
                attributeToDelete))
        .andExpect(status().isBadRequest());
  }

  @Test
  @Transactional
  void describeType() throws Exception {
    RecordType type = RecordType.valueOf("recordType");

    RecordType referencedType = RecordType.valueOf("referencedType");
    createSomeRecords(referencedType, 3);
    createSomeRecords(type, 1);
    RecordAttributes attributes = RecordAttributes.empty();
    String ref = RelationUtils.createRelationString(referencedType, "record_0");
    List<String> relArr =
        IntStream.range(0, 3)
            .mapToObj(Integer::toString)
            .map(i -> RelationUtils.createRelationString(referencedType, "record_" + i))
            .collect(Collectors.toList());
    attributes.putAttribute("array-of-relation", relArr);
    attributes.putAttribute("attr-ref", ref);

    mockMvc
        .perform(
            patch(
                    "/{instanceId}/records/{version}/{recordType}/{recordId}",
                    instanceId,
                    versionId,
                    type,
                    "record_0")
                .contentType(MediaType.APPLICATION_JSON)
                .content(toJson(new RecordRequest(attributes))))
        .andExpect(status().isOk())
        .andExpect(content().string(containsString(ref)));

    List<AttributeSchema> expectedAttributes =
        Arrays.asList(
            new AttributeSchema("array-of-date", DataTypeMapping.ARRAY_OF_DATE),
            new AttributeSchema("array-of-datetime", DataTypeMapping.ARRAY_OF_DATE_TIME),
            new AttributeSchema(
                "array-of-relation", DataTypeMapping.ARRAY_OF_RELATION, referencedType),
            new AttributeSchema("array-of-string", DataTypeMapping.ARRAY_OF_STRING),
            new AttributeSchema("attr-boolean", DataTypeMapping.BOOLEAN),
            new AttributeSchema("attr-dt", DataTypeMapping.DATE_TIME),
            new AttributeSchema("attr-json", DataTypeMapping.JSON),
            new AttributeSchema("attr-ref", DataTypeMapping.RELATION, referencedType),
            new AttributeSchema("attr1", DataTypeMapping.STRING),
            new AttributeSchema("attr2", DataTypeMapping.NUMBER),
            new AttributeSchema("attr3", DataTypeMapping.DATE),
            new AttributeSchema("attr4", DataTypeMapping.STRING),
            new AttributeSchema("attr5", DataTypeMapping.NUMBER),
            new AttributeSchema("sys_name", DataTypeMapping.STRING),
            new AttributeSchema("z-array-of-boolean", DataTypeMapping.ARRAY_OF_BOOLEAN),
            new AttributeSchema("z-array-of-number-double", DataTypeMapping.ARRAY_OF_NUMBER),
            new AttributeSchema("z-array-of-number-long", DataTypeMapping.ARRAY_OF_NUMBER),
            new AttributeSchema("z-array-of-string", DataTypeMapping.ARRAY_OF_STRING));

    RecordTypeSchema expected = new RecordTypeSchema(type, expectedAttributes, 1, RECORD_ID);

    MvcResult mvcResult =
        mockMvc
            .perform(get("/{instanceId}/types/{v}/{type}", instanceId, versionId, type))
            .andExpect(status().isOk())
            .andReturn();

    RecordTypeSchema actual = fromJson(mvcResult, RecordTypeSchema.class);

    assertEquals(expected, actual);
  }

  @Test
  @Transactional
  void incompatibleArrayWritesShouldChangeToStringArray() throws Exception {
    String recordType = "test-type";
    List<Record> someRecords = createSomeRecords(recordType, 1);
    RecordRequest recordRequest =
        new RecordRequest(
            someRecords
                .get(0)
                .getAttributes()
                .putAttribute("array-of-date", List.of("should switch to array of string")));
    mockMvc
        .perform(
            put(
                    "/{instanceId}/records/{version}/{recordType}/{recordId}",
                    instanceId,
                    versionId,
                    recordType,
                    "new_id")
                .content(toJson(recordRequest))
                .contentType(MediaType.APPLICATION_JSON))
        .andExpect(status().is2xxSuccessful());
    MvcResult mvcResult =
        mockMvc
            .perform(get("/{instanceId}/types/{v}/{type}", instanceId, versionId, recordType))
            .andExpect(status().isOk())
            .andReturn();
    RecordTypeSchema actual = fromJson(mvcResult, RecordTypeSchema.class);
    assertEquals("ARRAY_OF_STRING", actual.attributes().get(0).datatype());
  }

  @Test
  @Transactional
  void describeNonexistentType() throws Exception {
    mockMvc
        .perform(get("/{instanceId}/types/{v}/{type}", instanceId, versionId, "noType"))
        .andExpect(status().isNotFound());
  }

  @Test
  @Transactional
  void describeAllTypes() throws Exception {
    RecordType type1 = RecordType.valueOf("firstType");
    createSomeRecords(type1, 1, instanceId);
    RecordType type2 = RecordType.valueOf("secondType");
    createSomeRecords(type2, 2, instanceId);
    RecordType type3 = RecordType.valueOf("thirdType");
    createSomeRecords(type3, 10, instanceId);

    List<AttributeSchema> expectedAttributes =
        Arrays.asList(
            new AttributeSchema("array-of-date", DataTypeMapping.ARRAY_OF_DATE),
            new AttributeSchema("array-of-datetime", DataTypeMapping.ARRAY_OF_DATE_TIME),
            new AttributeSchema("array-of-string", DataTypeMapping.ARRAY_OF_STRING),
            new AttributeSchema("attr-boolean", DataTypeMapping.BOOLEAN),
            new AttributeSchema("attr-dt", DataTypeMapping.DATE_TIME),
            new AttributeSchema("attr-json", DataTypeMapping.JSON),
            new AttributeSchema("attr1", DataTypeMapping.STRING),
            new AttributeSchema("attr2", DataTypeMapping.NUMBER),
            new AttributeSchema("attr3", DataTypeMapping.DATE),
            new AttributeSchema("attr4", DataTypeMapping.STRING),
            new AttributeSchema("attr5", DataTypeMapping.NUMBER),
            new AttributeSchema("sys_name", DataTypeMapping.STRING),
            new AttributeSchema("z-array-of-boolean", DataTypeMapping.ARRAY_OF_BOOLEAN),
            new AttributeSchema("z-array-of-number-double", DataTypeMapping.ARRAY_OF_NUMBER),
            new AttributeSchema("z-array-of-number-long", DataTypeMapping.ARRAY_OF_NUMBER),
            new AttributeSchema("z-array-of-string", DataTypeMapping.ARRAY_OF_STRING));

    List<RecordTypeSchema> expectedSchemas =
        Arrays.asList(
            new RecordTypeSchema(type1, expectedAttributes, 1, RECORD_ID),
            new RecordTypeSchema(type2, expectedAttributes, 2, RECORD_ID),
            new RecordTypeSchema(type3, expectedAttributes, 10, RECORD_ID));

    MvcResult mvcResult =
        mockMvc
            .perform(get("/{instanceId}/types/{v}", instanceId, versionId))
            .andExpect(status().isOk())
            .andReturn();

    List<RecordTypeSchema> actual = Arrays.asList(fromJson(mvcResult, RecordTypeSchema[].class));

    assertEquals(expectedSchemas, actual);
  }

  @Test
  @Transactional
  void describeAllTypesNoInstance() throws Exception {
    mockMvc
        .perform(get("/{instanceId}/types/{v}", UUID.randomUUID(), versionId))
        .andExpect(status().isNotFound());
  }

  private List<Record> createSomeRecords(String recordType, int numRecords) throws Exception {
    return createSomeRecords(RecordType.valueOf(recordType), numRecords, instanceId);
  }

  private List<Record> createSomeRecords(RecordType recordType, int numRecords) throws Exception {
    return createSomeRecords(recordType, numRecords, instanceId);
  }

  private List<Record> createSomeRecords(RecordType recordType, int numRecords, UUID instId)
      throws Exception {
    List<Record> result = new ArrayList<>();
    for (int i = 0; i < numRecords; i++) {
      String recordId = "record_" + i;
      RecordAttributes attributes = generateRandomAttributes();
      RecordRequest recordRequest = new RecordRequest(attributes);
      mockMvc
          .perform(
              put(
                      "/{instanceId}/records/{version}/{recordType}/{recordId}",
                      instId,
                      versionId,
                      recordType,
                      recordId)
                  .content(toJson(recordRequest))
                  .contentType(MediaType.APPLICATION_JSON))
          .andExpect(status().is2xxSuccessful());
      result.add(new Record(recordId, recordType, recordRequest));
    }
    return result;
  }

  @Test
  @Transactional
  void batchWriteInsertShouldSucceed() throws Exception {
    String recordId = "foo";
    String newBatchRecordType = "new-record-type";
    Record record =
        new Record(
            recordId,
            RecordType.valueOf(newBatchRecordType),
            new RecordAttributes(Map.of("attr1", "attr-val")));
    Record record2 =
        new Record(
            "foo2",
            RecordType.valueOf(newBatchRecordType),
            new RecordAttributes(Map.of("attr1", "attr-val")));
    BatchOperation op = new BatchOperation(record, OperationType.UPSERT);
    mockMvc
        .perform(
            post("/{instanceid}/batch/{v}/{type}", instanceId, versionId, newBatchRecordType)
                .content(toJson(List.of(op, new BatchOperation(record2, OperationType.UPSERT))))
                .contentType(MediaType.APPLICATION_JSON))
        .andExpect(jsonPath("$.recordsModified", is(2)))
        .andExpect(jsonPath("$.message", is("Huzzah")))
        .andExpect(status().isOk());
    mockMvc
        .perform(
            get(
                    "/{instanceId}/records/{version}/{recordType}/{recordId}",
                    instanceId,
                    versionId,
                    newBatchRecordType,
                    recordId)
                .contentType(MediaType.APPLICATION_JSON))
        .andExpect(status().isOk());
    mockMvc
        .perform(
            post("/{instanceid}/batch/{v}/{type}", instanceId, versionId, newBatchRecordType)
                .content(toJson(List.of(new BatchOperation(record, OperationType.DELETE))))
                .contentType(MediaType.APPLICATION_JSON))
        .andExpect(status().isOk());
    mockMvc
        .perform(
            get(
                    "/{instanceId}/records/{version}/{recordType}/{recordId}",
                    instanceId,
                    versionId,
                    newBatchRecordType,
                    recordId)
                .contentType(MediaType.APPLICATION_JSON))
        .andExpect(status().isNotFound());
  }

  @Test
  @Transactional
  void batchWriteInsertShouldSucceedIfMultipleBatches(
      @Value("${twds.write.batch.size}") int batchSize) throws Exception {
    int totalRecords = Math.round(batchSize * 1.5f);
    RecordType newBatchRecordType = RecordType.valueOf("multi-batch");
    List<BatchOperation> ops = new ArrayList();
    for (int i = 0; i < batchSize * 1.5; i++) {
      String recordId = "record_" + i;
      Record record =
          new Record(
              recordId, newBatchRecordType, new RecordAttributes(Map.of("key", "value_" + i)));
      ops.add(new BatchOperation(record, OperationType.UPSERT));
    }

    mockMvc
        .perform(
            post("/{instanceid}/batch/{v}/{type}", instanceId, versionId, newBatchRecordType)
                .content(toJson(ops))
                .contentType(MediaType.APPLICATION_JSON))
        .andExpect(jsonPath("$.recordsModified", is(totalRecords)))
        .andExpect(jsonPath("$.message", is("Huzzah")))
        .andExpect(status().isOk());
  }

  @Test
  @Transactional
  void batchWriteWithRelationsShouldSucceed() throws Exception {
    RecordType referenced = RecordType.valueOf("referenced");
    createSomeRecords(referenced, 4);
    String recordId = "foo";
    String newBatchRecordType = "new-record-type";
    List<BatchOperation> ops = new ArrayList<>();
    for (int i = 0; i < 3; i++) {
      Record record =
          new Record(
              recordId + i,
              RecordType.valueOf(newBatchRecordType),
              new RecordAttributes(
                  Map.of(
                      "relArr",
                      List.of(
                          RelationUtils.createRelationString(referenced, "record_" + i),
                          RelationUtils.createRelationString(referenced, "record_" + (i + 1))))));
      ops.add(new BatchOperation(record, OperationType.UPSERT));
    }
    mockMvc
        .perform(
            post("/{instanceid}/batch/{v}/{type}", instanceId, versionId, newBatchRecordType)
                .content(toJson(ops))
                .contentType(MediaType.APPLICATION_JSON))
        .andExpect(jsonPath("$.recordsModified", is(3)))
        .andExpect(jsonPath("$.message", is("Huzzah")))
        .andExpect(status().isOk());
    MvcResult mvcResult =
        mockMvc
            .perform(
                get(
                        "/{instanceId}/records/{version}/{recordType}/{recordId}",
                        instanceId,
                        versionId,
                        newBatchRecordType,
                        recordId + "0")
                    .contentType(MediaType.APPLICATION_JSON))
            .andExpect(status().isOk())
            .andReturn();
    RecordResponse actualSingle = fromJson(mvcResult, RecordResponse.class);
    assertEquals(2, actualSingle.recordAttributes().attributeSet().size());
    assertTrue(
        actualSingle
            .recordAttributes()
            .getAttributeValue("relArr")
            .toString()
            .contains(RelationUtils.createRelationString(referenced, "record_1")));
  }

  @Test
  @Transactional
  void batchInsertShouldFailWithInvalidRelation() throws Exception {
    RecordType recordType = RecordType.valueOf("relationBatchInsert");
    List<BatchOperation> batchOperations =
        List.of(
            new BatchOperation(
                new Record(
                    "record_0",
                    recordType,
                    new RecordAttributes(
                        Map.of(
                            "attr-relation",
                            RelationUtils.createRelationString(
                                RecordType.valueOf("missing"), "A")))),
                OperationType.UPSERT),
            new BatchOperation(
                new Record(
                    "record_1",
                    recordType,
                    new RecordAttributes(
                        Map.of(
                            "attr-relation",
                            RelationUtils.createRelationString(
                                RecordType.valueOf("missing"), "A")))),
                OperationType.UPSERT));
    mockMvc
        .perform(
            post("/{instanceid}/batch/{v}/{type}", instanceId, versionId, recordType)
                .content(toJson(batchOperations))
                .contentType(MediaType.APPLICATION_JSON))
        .andExpect(status().isNotFound());
  }

  @Test
  @Transactional
  void batchInsertShouldFailWithInvalidRelationExistingRecordType() throws Exception {
    RecordType recordType = RecordType.valueOf("relationBatchInsert");
    createSomeRecords(recordType, 2);
    List<BatchOperation> batchOperations =
        List.of(
            new BatchOperation(
                new Record(
                    "record_0",
                    recordType,
                    new RecordAttributes(
                        Map.of(
                            "attr-relation",
                            RelationUtils.createRelationString(
                                RecordType.valueOf("missing"), "A")))),
                OperationType.UPSERT),
            new BatchOperation(
                new Record(
                    "record_1",
                    recordType,
                    new RecordAttributes(
                        Map.of(
                            "attr-relation",
                            RelationUtils.createRelationString(
                                RecordType.valueOf("missing"), "A")))),
                OperationType.UPSERT));
    mockMvc
        .perform(
            post("/{instanceid}/batch/{v}/{type}", instanceId, versionId, recordType)
                .content(toJson(batchOperations))
                .contentType(MediaType.APPLICATION_JSON))
        .andExpect(status().isNotFound());
  }

  @Test
  @Transactional
  void mixOfUpsertAndDeleteShouldSucceed() throws Exception {
    RecordType recordType = RecordType.valueOf("forBatch");
    List<Record> records = createSomeRecords(recordType, 2);
    Record upsertRcd = records.get(1);
    upsertRcd.getAttributes().putAttribute("new-col", "new value!!");
    List<BatchOperation> ops =
        List.of(
            new BatchOperation(records.get(0), OperationType.DELETE),
            new BatchOperation(upsertRcd, OperationType.UPSERT));
    mockMvc
        .perform(
            post("/{instanceid}/batch/{v}/{type}", instanceId, versionId, recordType)
                .content(toJson(ops))
                .contentType(MediaType.APPLICATION_JSON))
        .andExpect(status().isOk());
    mockMvc
        .perform(
            get(
                    "/{instanceId}/records/{version}/{recordType}/{recordId}",
                    instanceId,
                    versionId,
                    recordType,
                    records.get(0).getId())
                .contentType(MediaType.APPLICATION_JSON))
        .andExpect(status().isNotFound());
    mockMvc
        .perform(
            get(
                    "/{instanceId}/records/{version}/{recordType}/{recordId}",
                    instanceId,
                    versionId,
                    recordType,
                    upsertRcd.getId())
                .contentType(MediaType.APPLICATION_JSON))
        .andExpect(status().isOk())
        .andExpect(jsonPath("$.attributes.new-col", is("new value!!")));
  }

  @Test
  @Transactional
  void dateAttributeShouldBeHumanReadable() throws Exception {
    // N.B. This test does not assert that the date attribute is saved as a date in
    // Postgres;
    // other tests verify that.
    RecordType recordType = RecordType.valueOf("test-type");
    RecordAttributes attributes = RecordAttributes.empty();
    String dateString = "1911-01-21";
    attributes.putAttribute("dateAttr", dateString);
    // create record in db
    mockMvc
        .perform(
            put(
                    "/{instanceId}/records/{version}/{recordType}/{recordId}",
                    instanceId,
                    versionId,
                    recordType,
                    "recordId")
                .content(toJson(new RecordRequest(attributes)))
                .contentType(MediaType.APPLICATION_JSON))
        .andExpect(status().isCreated());
    // retrieve as single record
    MvcResult mvcSingleResult =
        mockMvc
            .perform(
                get(
                    "/{instanceId}/records/{version}/{recordType}/{recordId}",
                    instanceId,
                    versionId,
                    recordType,
                    "recordId"))
            .andExpect(status().isOk())
            .andReturn();
    // assert single-record response is human-readable
    RecordResponse actualSingle = fromJson(mvcSingleResult, RecordResponse.class);
    assertEquals(dateString, actualSingle.recordAttributes().getAttributeValue("dateAttr"));

    // retrieve as a page of records
    MvcResult mvcMultiResult =
        mockMvc
            .perform(
                post(
                    "/{instanceId}/search/{version}/{recordType}",
                    instanceId,
                    versionId,
                    recordType))
            .andExpect(status().isOk())
            .andReturn();

    RecordQueryResponse actualMulti = fromJson(mvcMultiResult, RecordQueryResponse.class);
    assertEquals(
        dateString, actualMulti.records().get(0).recordAttributes().getAttributeValue("dateAttr"));
  }

  @Test
  @Transactional
  void datetimeAttributeShouldBeHumanReadable() throws Exception {
    // N.B. This test does not assert that the datetime attribute is saved as a
    // timestamp in Postgres;
    // other tests verify that.
    RecordType recordType = RecordType.valueOf("test-type");
    RecordAttributes attributes = RecordAttributes.empty();
    String datetimeString = "1911-01-21T13:45:43";
    attributes.putAttribute("datetimeAttr", datetimeString);
    // create record in db
    mockMvc
        .perform(
            put(
                    "/{instanceId}/records/{version}/{recordType}/{recordId}",
                    instanceId,
                    versionId,
                    recordType,
                    "recordId")
                .content(toJson(new RecordRequest(attributes)))
                .contentType(MediaType.APPLICATION_JSON))
        .andExpect(status().isCreated());
    // retrieve as single record
    MvcResult mvcSingleResult =
        mockMvc
            .perform(
                get(
                    "/{instanceId}/records/{version}/{recordType}/{recordId}",
                    instanceId,
                    versionId,
                    recordType,
                    "recordId"))
            .andExpect(status().isOk())
            .andReturn();
    // assert single-record response is human-readable
    RecordResponse actualSingle = fromJson(mvcSingleResult, RecordResponse.class);
    assertEquals(datetimeString, actualSingle.recordAttributes().getAttributeValue("datetimeAttr"));

    // retrieve as a page of records
    MvcResult mvcMultiResult =
        mockMvc
            .perform(
                post(
                    "/{instanceId}/search/{version}/{recordType}",
                    instanceId,
                    versionId,
                    recordType))
            .andExpect(status().isOk())
            .andReturn();

    RecordQueryResponse actualMulti = fromJson(mvcMultiResult, RecordQueryResponse.class);
    assertEquals(
        datetimeString,
        actualMulti.records().get(0).recordAttributes().getAttributeValue("datetimeAttr"));
  }

  @Test
  @Transactional
  void testUpdateRecordWithRelationArray() throws Exception {
    // add some records to be relations
    RecordType recordType = RecordType.valueOf("referencedRecords");
    createSomeRecords(recordType, 3);

    // Create record with relation array
    RecordType relationArrayType = RecordType.valueOf("relationArrayType");
    String relArrId = "recordWithRelationArr";
    List<String> relArr =
        List.of(
            RelationUtils.createRelationString(recordType, "record_0"),
            RelationUtils.createRelationString(recordType, "record_1"));
    Record recordWithRelationArray =
        new Record(relArrId, relationArrayType, new RecordAttributes(Map.of("relArrAttr", relArr)));
    RecordAttributes relAttr = new RecordAttributes(Map.of("relArrAttr", relArr));
    mockMvc
        .perform(
            put(
                    "/{instanceId}/records/{version}/{recordType}/{recordId}",
                    instanceId,
                    versionId,
                    relationArrayType,
                    relArrId)
                .contentType(MediaType.APPLICATION_JSON)
                .content(toJson(new RecordRequest(relAttr))))
        .andExpect(status().isCreated())
        .andExpect(jsonPath("$.attributes.relArrAttr", is(relArr)));

    // Update relation array
    relArr =
        List.of(
            RelationUtils.createRelationString(recordType, "record_0"),
            RelationUtils.createRelationString(recordType, "record_2"));
    relAttr = new RecordAttributes(Map.of("relArrAttr", relArr));
    mockMvc
        .perform(
            patch(
                    "/{instanceId}/records/{versionId}/{recordType}/{recordId}",
                    instanceId,
                    versionId,
                    relationArrayType,
                    relArrId)
                .contentType(MediaType.APPLICATION_JSON)
                .content(toJson(new RecordRequest(relAttr))))
        .andExpect(
            content()
                .string(containsString(RelationUtils.createRelationString(recordType, "record_2"))))
        .andExpect(
            content()
                .string(
                    not(
                        containsString(
                            RelationUtils.createRelationString(recordType, "record_1")))))
        .andExpect(status().isOk());

    MvcResult result =
        mockMvc
            .perform(
                get(
                    "/{instanceId}/records/{version}/{recordType}/{recordId}",
                    instanceId,
                    versionId,
                    relationArrayType,
                    relArrId))
            .andExpect(status().isOk())
            .andReturn();
    RecordResponse recordResponse = fromJson(result, RecordResponse.class);
    List<String> actualAttrValue =
        assertInstanceOf(
            List.class, recordResponse.recordAttributes().getAttributeValue("relArrAttr"));
    assertIterableEquals(relArr, actualAttrValue);

    // TODO: a better way to check join table?
    // Join table should have been updated
    List<String> joinVals =
        testDao.getRelationArrayValues(
            instanceId, "relArrAttr", recordWithRelationArray, recordType);
    assertIterableEquals(List.of("record_0", "record_2"), joinVals);
  }

  // Note: this test is not transactional so that I can make further calls to the DB
  // After the patch call has already thrown an exception
  @Test
  void testUpdateRelationArrayWithInvalidRecord() throws Exception {
    // add some records to be relations
    RecordType recordType = RecordType.valueOf("referencedRecords");
    createSomeRecords(recordType, 2);

    // Create record with relation array
    RecordType relationArrayType = RecordType.valueOf("relationArrayType");
    String relArrId = "recordWithRelationArr";
    List<String> relArr =
        List.of(
            RelationUtils.createRelationString(recordType, "record_0"),
            RelationUtils.createRelationString(recordType, "record_1"));
    Record recordWithRelationArray =
        new Record(relArrId, relationArrayType, new RecordAttributes(Map.of("relArrAttr", relArr)));
    RecordAttributes relAttr = new RecordAttributes(Map.of("relArrAttr", relArr));
    mockMvc
        .perform(
            put(
                    "/{instanceId}/records/{version}/{recordType}/{recordId}",
                    instanceId,
                    versionId,
                    relationArrayType,
                    relArrId)
                .contentType(MediaType.APPLICATION_JSON)
                .content(toJson(new RecordRequest(relAttr))))
        .andExpect(status().isCreated())
        .andExpect(jsonPath("$.attributes.relArrAttr", is(relArr)));

    // Update relation array with invalid record
    List<String> incorrectArr =
        List.of(
            RelationUtils.createRelationString(recordType, "record_0"),
            RelationUtils.createRelationString(recordType, "invalidId"));
    RecordAttributes incorrectRelAttr = new RecordAttributes(Map.of("relArrAttr", incorrectArr));
    mockMvc
        .perform(
            patch(
                    "/{instanceId}/records/{versionId}/{recordType}/{recordId}",
                    instanceId,
                    versionId,
                    relationArrayType,
                    relArrId)
                .contentType(MediaType.APPLICATION_JSON)
                .content(toJson(new RecordRequest(incorrectRelAttr))))
        .andExpect(status().isForbidden());

    // Record should not have been updated
    MvcResult result =
        mockMvc
            .perform(
                get(
                    "/{instanceId}/records/{version}/{recordType}/{recordId}",
                    instanceId,
                    versionId,
                    relationArrayType,
                    relArrId))
            .andExpect(status().isOk())
            .andReturn();
    RecordResponse recordResponse = fromJson(result, RecordResponse.class);
    List<String> actualAttrValue =
        assertInstanceOf(
            List.class, recordResponse.recordAttributes().getAttributeValue("relArrAttr"));
    assertIterableEquals(relArr, actualAttrValue);

    // Join table should not have been updated
    List<String> joinVals =
        testDao.getRelationArrayValues(
            instanceId, "relArrAttr", recordWithRelationArray, recordType);
    assertIterableEquals(List.of("record_0", "record_1"), joinVals);
  }

  @Test
  @Transactional
  void testUpdateRelationArrayWithMismatchedType() throws Exception {
    // add some records to be relations
    RecordType recordType = RecordType.valueOf("referencedRecords");
    createSomeRecords(recordType, 3);

    RecordType secondType = RecordType.valueOf("secondReference");
    createSomeRecords(secondType, 1);

    // Create record with relation array
    RecordType relationArrayType = RecordType.valueOf("relationArrayType");
    String relArrId = "recordWithRelationArr";
    List<String> relArr =
        List.of(
            RelationUtils.createRelationString(recordType, "record_1"),
            RelationUtils.createRelationString(recordType, "record_2"));
    Record recordWithRelationArray =
        new Record(relArrId, relationArrayType, new RecordAttributes(Map.of("relArrAttr", relArr)));
    RecordAttributes relAttr = new RecordAttributes(Map.of("relArrAttr", relArr));
    mockMvc
        .perform(
            put(
                    "/{instanceId}/records/{version}/{recordType}/{recordId}",
                    instanceId,
                    versionId,
                    relationArrayType,
                    relArrId)
                .contentType(MediaType.APPLICATION_JSON)
                .content(toJson(new RecordRequest(relAttr))))
        .andExpect(status().isCreated())
        .andExpect(jsonPath("$.attributes.relArrAttr", is(relArr)));

    // Update relation array with mismatched record type
    List<String> incorrectArr =
        List.of(
            RelationUtils.createRelationString(recordType, "record_0"),
            RelationUtils.createRelationString(secondType, "record_0"));
    RecordAttributes incorrectRelAttr = new RecordAttributes(Map.of("relArrAttr", incorrectArr));
    mockMvc
        .perform(
            patch(
                    "/{instanceId}/records/{versionId}/{recordType}/{recordId}",
                    instanceId,
                    versionId,
                    relationArrayType,
                    relArrId)
                .contentType(MediaType.APPLICATION_JSON)
                .content(toJson(new RecordRequest(incorrectRelAttr))))
        .andExpect(status().isForbidden());

    // Record should not have been updated
    MvcResult result =
        mockMvc
            .perform(
                get(
                    "/{instanceId}/records/{version}/{recordType}/{recordId}",
                    instanceId,
                    versionId,
                    relationArrayType,
                    relArrId))
            .andExpect(status().isOk())
            .andReturn();
    RecordResponse recordResponse = fromJson(result, RecordResponse.class);
    List<String> actualAttrValue =
        assertInstanceOf(
            List.class, recordResponse.recordAttributes().getAttributeValue("relArrAttr"));
    assertIterableEquals(relArr, actualAttrValue);

    List<String> joinVals =
        testDao.getRelationArrayValues(
            instanceId, "relArrAttr", recordWithRelationArray, recordType);
    assertIterableEquals(List.of("record_1", "record_2"), joinVals);
  }

  @Test
  void testUpdateRelationArrayDataType() throws Exception {
    // add some records to be relations
    RecordType recordType = RecordType.valueOf("referencedRecords");
    createSomeRecords(recordType, 2);

    // Create record with relation array
    RecordType relationArrayType = RecordType.valueOf("relationArrayType");
    String relArrId = "recordWithRelationArr";
    List<String> relArr =
        List.of(
            RelationUtils.createRelationString(recordType, "record_0"),
            RelationUtils.createRelationString(recordType, "record_1"));
    Record recordWithRelationArray =
        new Record(relArrId, relationArrayType, new RecordAttributes(Map.of("relArrAttr", relArr)));
    RecordAttributes relAttr = new RecordAttributes(Map.of("relArrAttr", relArr));
    mockMvc
        .perform(
            put(
                    "/{instanceId}/records/{version}/{recordType}/{recordId}",
                    instanceId,
                    versionId,
                    relationArrayType,
                    relArrId)
                .contentType(MediaType.APPLICATION_JSON)
                .content(toJson(new RecordRequest(relAttr))))
        .andExpect(status().isCreated())
        .andExpect(jsonPath("$.attributes.relArrAttr", is(relArr)));

    // Update relation array attribute to non-relation array datatype
    RecordAttributes incorrectRelAttr =
        new RecordAttributes(Map.of("relArrAttr", "not an array of relations"));
    mockMvc
        .perform(
            patch(
                    "/{instanceId}/records/{versionId}/{recordType}/{recordId}",
                    instanceId,
                    versionId,
                    relationArrayType,
                    relArrId)
                .contentType(MediaType.APPLICATION_JSON)
                .content(toJson(new RecordRequest(incorrectRelAttr))))
        .andExpect(status().isForbidden());

    // Record should not have been updated
    MvcResult result =
        mockMvc
            .perform(
                get(
                    "/{instanceId}/records/{version}/{recordType}/{recordId}",
                    instanceId,
                    versionId,
                    relationArrayType,
                    relArrId))
            .andExpect(status().isOk())
            .andReturn();
    RecordResponse recordResponse = fromJson(result, RecordResponse.class);
    List<String> actualAttrValue =
        assertInstanceOf(
            List.class, recordResponse.recordAttributes().getAttributeValue("relArrAttr"));
    assertIterableEquals(relArr, actualAttrValue);

    // Join table should not have been updated
    List<String> joinVals =
        testDao.getRelationArrayValues(
            instanceId, "relArrAttr", recordWithRelationArray, recordType);
    assertIterableEquals(List.of("record_0", "record_1"), joinVals);
  }

  @ParameterizedTest
  @MethodSource("jsonValues")
  @Transactional
  void createRecordWithJsonAttributes(Object inputValue, Object expectedValue) throws Exception {
    RecordType recordType = RecordType.valueOf("test-type");
    String recordId = "recordId";
    String attributeName = "jsonValue";
    RecordAttributes attributes = RecordAttributes.empty().putAttribute(attributeName, inputValue);

    mockMvc
        .perform(
            put(
                    "/{instanceId}/records/{version}/{recordType}/{recordId}",
                    instanceId,
                    versionId,
                    recordType,
                    recordId)
                .content(toJson(new RecordRequest(attributes)))
                .contentType(MediaType.APPLICATION_JSON))
        .andExpect(status().isCreated());

    MvcResult mvcSingleResult =
        mockMvc
            .perform(
                get(
                    "/{instanceId}/records/{version}/{recordType}/{recordId}",
                    instanceId,
                    versionId,
                    recordType,
                    recordId))
            .andExpect(status().isOk())
            .andReturn();
    RecordResponse record = fromJson(mvcSingleResult, RecordResponse.class);

    assertEquals(expectedValue, record.recordAttributes().getAttributeValue(attributeName));

    MvcResult schemaResult =
        mockMvc
            .perform(get("/{instanceid}/types/{v}/{type}", instanceId, versionId, recordType))
            .andReturn();
    RecordTypeSchema schema = fromJson(schemaResult, RecordTypeSchema.class);

    assertEquals("JSON", schema.getAttributeSchema(attributeName).datatype());
  }

  static Stream<Arguments> jsonValues() {
    return Stream.of(
        Arguments.of(
            Map.of("string", "foo", "number", 1, "boolean", true),
            Map.of("string", "foo", "number", BigInteger.valueOf(1), "boolean", Boolean.TRUE)),
        Arguments.of(
            "{\"string\":\"foo\",\"number\":1,\"boolean\":true}",
            Map.of("string", "foo", "number", BigInteger.valueOf(1), "boolean", Boolean.TRUE)),
        Arguments.of(
            List.of(
                Map.of("string", "foo", "number", 1, "boolean", true),
                Map.of("string", "bar", "number", 2, "boolean", false)),
            List.of(
                Map.of("string", "foo", "number", BigInteger.valueOf(1), "boolean", Boolean.TRUE),
                Map.of(
                    "string", "bar", "number", BigInteger.valueOf(2), "boolean", Boolean.FALSE))),
        Arguments.of(
            "[{\"string\":\"foo\",\"number\":1,\"boolean\":true},{\"string\":\"bar\",\"number\":2,\"boolean\":false}]",
            List.of(
                Map.of("string", "foo", "number", BigInteger.valueOf(1), "boolean", Boolean.TRUE),
                Map.of(
                    "string", "bar", "number", BigInteger.valueOf(2), "boolean", Boolean.FALSE))));
  }
}
