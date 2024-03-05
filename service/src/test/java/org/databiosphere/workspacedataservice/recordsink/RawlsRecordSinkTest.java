package org.databiosphere.workspacedataservice.recordsink;

import static com.google.common.collect.MoreCollectors.onlyElement;
import static java.util.Arrays.stream;
import static java.util.Collections.emptyMap;
import static java.util.stream.Stream.concat;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import java.io.IOException;
import java.io.StringWriter;
import java.nio.charset.StandardCharsets;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.databiosphere.workspacedataservice.common.TestBase;
import org.databiosphere.workspacedataservice.dataimport.ImportDetails;
import org.databiosphere.workspacedataservice.pubsub.PubSub;
import org.databiosphere.workspacedataservice.recordsink.RawlsModel.AddListMember;
import org.databiosphere.workspacedataservice.recordsink.RawlsModel.AddUpdateAttribute;
import org.databiosphere.workspacedataservice.recordsink.RawlsModel.AttributeOperation;
import org.databiosphere.workspacedataservice.recordsink.RawlsModel.CreateAttributeValueList;
import org.databiosphere.workspacedataservice.recordsink.RawlsModel.Entity;
import org.databiosphere.workspacedataservice.recordsink.RawlsModel.Op;
import org.databiosphere.workspacedataservice.recordsink.RawlsModel.RemoveAttribute;
import org.databiosphere.workspacedataservice.shared.model.Record;
import org.databiosphere.workspacedataservice.shared.model.RecordAttributes;
import org.databiosphere.workspacedataservice.shared.model.RecordType;
import org.databiosphere.workspacedataservice.storage.GcsStorage;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.util.StreamUtils;

@DirtiesContext
@SpringBootTest
@ActiveProfiles(value = "control-plane", inheritProfiles = false)
class RawlsRecordSinkTest extends TestBase {
  @Autowired private ObjectMapper mapper;
  @MockBean private PubSub pubSub;
  private RecordSink recordSink;
  private StringWriter recordedJson;

  @Qualifier("mockGcsStorage")
  @Autowired
  private GcsStorage storage;

  private final UUID WORKSPACE_ID = UUID.randomUUID();
  private final UUID JOB_ID = UUID.randomUUID();
  private final String USER_EMAIL = "userEmail";

  @BeforeEach
  void setUp() {
    recordedJson = new StringWriter();
    recordSink =
        new RawlsRecordSink(
            mapper,
            json -> recordedJson.append(json),
            storage,
            pubSub,
            new ImportDetails(JOB_ID, USER_EMAIL, WORKSPACE_ID, "prefix"));
  }

  @Test
  void translatesEntityFields() {
    var entities = doUpsert(makeRecord(/* type= */ "widget", /* id= */ "id", emptyMap()));

    var entity = assertSingle(entities);
    assertThat(entity.name()).isEqualTo("id");
    assertThat(entity.entityType()).isEqualTo("widget");
  }

  @Test
  void prependsPrefixToAttributeName() {
    var entities =
        doUpsert(makeRecord(/* type= */ "widget", /* id= */ "id", Map.of("attrName", "attrValue")));

    var operation = assertSingleOperation(AddUpdateAttribute.class, entities);
    assertThat(operation.attributeName()).isEqualTo("prefix:attrName");
  }

  @Test
  void translatesAddUpdateAttribute() {
    var entities =
        doUpsert(makeRecord(/* type= */ "widget", /* id= */ "id", Map.of("someKey", "someValue")));

    var operation = assertSingleOperation(AddUpdateAttribute.class, entities);
    assertThat(operation.addUpdateAttribute()).isEqualTo("someValue");
  }

  @Test
  void ignoresNullAttributes() {
    Map<String, Object> nullAttributes = new HashMap<>();
    nullAttributes.put("someKey", null);
    nullAttributes.put("someOtherKey", null);
    var entities = doUpsert(makeRecord(/* type= */ "widget", /* id= */ "id", nullAttributes));

    var entity = assertSingle(entities);
    assertThat(entity.operations()).isEmpty();
  }

  @Test
  void translatesMultipleAddUpdateAttributes() {
    var entities =
        doUpsert(
            makeRecord(
                /* type= */ "widget",
                /* id= */ "id",
                new ImmutableMap.Builder<String, Object>()
                    .put("someKey", "someValue")
                    .put("someOtherKey", "someOtherValue")
                    .build()));

    var entity = assertSingle(entities);
    var operations = entity.operations();
    assertThat(operations).hasSize(2);
    assertThat(filterOperations(AddUpdateAttribute.class, operations))
        .containsExactly(
            new AddUpdateAttribute("prefix:someKey", "someValue"),
            new AddUpdateAttribute("prefix:someOtherKey", "someOtherValue"));
  }

  @Test
  void translatesArrayAttributes() {
    var entities =
        doUpsert(
            makeRecord(
                /* type= */ "widget",
                /* id= */ "id",
                Map.of("arrayKey", List.of("value1", "value2"))));

    var entity = assertSingle(entities);
    var operations = entity.operations();
    assertThat(operations).hasSize(4);
    assertThat(operations.stream().map(AttributeOperation::op).toList())
        .containsExactly(
            Op.REMOVE_ATTRIBUTE,
            Op.CREATE_ATTRIBUTE_VALUE_LIST,
            Op.ADD_LIST_MEMBER,
            Op.ADD_LIST_MEMBER);

    assertThat(filterOperations(RemoveAttribute.class, operations))
        .extracting(RemoveAttribute::attributeName)
        .containsExactly("prefix:arrayKey");

    assertThat(filterOperations(CreateAttributeValueList.class, operations))
        .extracting(CreateAttributeValueList::attributeName)
        .containsExactly("prefix:arrayKey");

    assertThat(filterOperations(AddListMember.class, operations))
        .containsExactly(
            new AddListMember("prefix:arrayKey", "value1"),
            new AddListMember("prefix:arrayKey", "value2"));
  }

  @Test
  void renamesNameToIncludeRecordType() {
    var entities =
        doUpsert(makeRecord(/* type= */ "widget", /* id= */ "id", Map.of("name", "nameValue")));

    var operation = assertSingleOperation(AddUpdateAttribute.class, entities);
    assertThat(operation.attributeName()).isEqualTo("prefix:widget_name");
  }

  @Test
  void batchDeleteNotSupported() {
    RecordType ignoredRecordType = RecordType.valueOf("widget");
    List<Record> ignoredEmptyRecords = List.of();
    var thrown =
        assertThrows(
            UnsupportedOperationException.class,
            () -> recordSink.deleteBatch(ignoredRecordType, ignoredEmptyRecords));
    assertThat(thrown).hasMessageContaining("does not support deleteBatch");
  }

  @Test
  void sendsPubSub() {
    doUpsert(makeRecord(/* type= */ "widget", /* id= */ "id", emptyMap()));

    Map<String, String> expectedMessage =
        Map.of(
            "workspaceId",
            WORKSPACE_ID.toString(),
            "userEmail",
            USER_EMAIL,
            "jobId",
            JOB_ID.toString(),
            "upsertFile",
            JOB_ID + ".rawlsUpsert",
            "isUpsert",
            "true",
            "isCWDS",
            "true");
    ArgumentCaptor<Map> argumentCaptor = ArgumentCaptor.forClass(Map.class);

    verify(pubSub, times(1)).publishSync(argumentCaptor.capture());

    assertThat(argumentCaptor.getValue()).isEqualTo(expectedMessage);
  }

  private Record makeRecord(String type, String id, Map<String, Object> attributes) {
    return new Record(id, RecordType.valueOf(type), new RecordAttributes(attributes));
  }

  private List<Entity> doUpsert(Record record, Record... additionalRecords) {
    var recordList = concat(Stream.of(record), stream(additionalRecords)).toList();
    var recordType = recordList.stream().map(Record::getRecordType).collect(onlyElement());
    var blobName = "";
    try {
      recordSink.upsertBatch(
          recordType,
          /* schema= */ Map.of(), // currently ignored
          recordList,
          /* primaryKey= */ "name" // currently ignored
          );

      assertThat(recordedJson.toString()).isNotNull();
      // look for the blob that was created in a bucket with the appropriate json
      var blobs = storage.getBlobsInBucket();
      // confirm there is only 1
      assertThat(Iterables.size(blobs)).isEqualTo(1);
      // get the name of the blob
      blobName = blobs.iterator().next().getName();
      // check that the contents match the expected Json
      var text = storage.getBlobContents(blobName);
      String contents = StreamUtils.copyToString(text, StandardCharsets.UTF_8);
      assertThat(contents).isEqualTo(recordedJson.toString());
      return mapper.readValue(recordedJson.toString(), new TypeReference<>() {});
    } catch (IOException e) {
      throw new RuntimeException(e);
    } finally {
      // delete the blob so the next test can have a clean slate
      if (!blobName.isEmpty()) {
        storage.deleteBlob(blobName);
      }
    }
  }

  // assert that the given collection has exactly one item, then return it
  private static <T> T assertSingle(Collection<T> elements) {
    return elements.stream().collect(onlyElement());
  }

  // assert that the given collection has exactly one item of the given type, then return it
  private static <T> T assertSingleInstanceOf(Class<T> type, Collection<?> elements) {
    return assertInstanceOf(type, assertSingle(elements));
  }

  // assert that the given entities contain exactly one entity with exactly one operation of the
  // given type, then return it
  private static <T> T assertSingleOperation(Class<T> type, Collection<Entity> entities) {
    var entity = assertSingle(entities);
    return assertSingleInstanceOf(type, entity.operations());
  }

  private static <T extends AttributeOperation> List<T> filterOperations(
      Class<T> type, Collection<? extends AttributeOperation> operations) {
    return operations.stream()
        .filter(type::isInstance)
        .map(type::cast)
        .collect(Collectors.toList());
  }
}
