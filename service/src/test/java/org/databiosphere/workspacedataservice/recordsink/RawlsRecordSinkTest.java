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
import org.databiosphere.workspacedataservice.recordsink.RawlsAttributePrefixer.PrefixStrategy;
import org.databiosphere.workspacedataservice.recordsink.RawlsModel.AddListMember;
import org.databiosphere.workspacedataservice.recordsink.RawlsModel.AddUpdateAttribute;
import org.databiosphere.workspacedataservice.recordsink.RawlsModel.AttributeOperation;
import org.databiosphere.workspacedataservice.recordsink.RawlsModel.AttributeValue;
import org.databiosphere.workspacedataservice.recordsink.RawlsModel.CreateAttributeValueList;
import org.databiosphere.workspacedataservice.recordsink.RawlsModel.Entity;
import org.databiosphere.workspacedataservice.recordsink.RawlsModel.Op;
import org.databiosphere.workspacedataservice.recordsink.RawlsModel.RemoveAttribute;
import org.databiosphere.workspacedataservice.shared.model.Record;
import org.databiosphere.workspacedataservice.shared.model.RecordAttributes;
import org.databiosphere.workspacedataservice.shared.model.RecordType;
import org.databiosphere.workspacedataservice.storage.GcsStorage;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.util.StreamUtils;

@DirtiesContext
@SpringBootTest
@ActiveProfiles(value = "control-plane", inheritProfiles = false)
class RawlsRecordSinkTest extends TestBase {
  @Autowired private ObjectMapper mapper;
  @MockBean private PubSub pubSub;

  @Qualifier("mockGcsStorage")
  @Autowired
  private GcsStorage storage;

  private final UUID WORKSPACE_ID = UUID.randomUUID();
  private final UUID JOB_ID = UUID.randomUUID();
  private final String USER_EMAIL = "userEmail";

  @Test
  void translatesEntityFields() {
    var entities = doUpsert(makeRecord(/* type= */ "widget", /* id= */ "id", emptyMap()));

    var entity = assertSingle(entities);
    assertThat(entity.name()).isEqualTo("id");
    assertThat(entity.entityType()).isEqualTo("widget");
  }

  @Test
  void translatesAddUpdateAttribute() {
    var entities =
        doUpsert(makeRecord(/* type= */ "widget", /* id= */ "id", Map.of("someKey", "someValue")));

    var operation = assertSingleOperation(AddUpdateAttribute.class, entities);
    assertThat(operation.addUpdateAttribute().value()).isEqualTo("someValue");
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
            new AddUpdateAttribute("someKey", AttributeValue.of("someValue")),
            new AddUpdateAttribute("someOtherKey", AttributeValue.of("someOtherValue")));
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
        .containsExactly("arrayKey");

    assertThat(filterOperations(CreateAttributeValueList.class, operations))
        .extracting(CreateAttributeValueList::attributeName)
        .containsExactly("arrayKey");

    assertThat(filterOperations(AddListMember.class, operations))
        .containsExactly(
            new AddListMember("arrayKey", AttributeValue.of("value1")),
            new AddListMember("arrayKey", AttributeValue.of("value2")));
  }

  @Test
  void batchDeleteNotSupported() throws IOException {
    RecordType ignoredRecordType = RecordType.valueOf("widget");
    List<Record> ignoredEmptyRecords = List.of();
    try (RecordSink recordSink = newRecordSink()) {
      var thrown =
          assertThrows(
              UnsupportedOperationException.class,
              () -> recordSink.deleteBatch(ignoredRecordType, ignoredEmptyRecords));
      assertThat(thrown).hasMessageContaining("does not support deleteBatch");
    }
  }

  @Test
  void sendsPubSub() {
    doUpsert(makeRecord(/* type= */ "widget", /* id= */ "id", emptyMap()));

    Map<String, String> expectedMessage =
        new ImmutableMap.Builder<String, String>()
            .put("workspaceId", WORKSPACE_ID.toString())
            .put("userEmail", USER_EMAIL)
            .put("jobId", JOB_ID.toString())
            .put("upsertFile", storage.getBucketName() + "/" + JOB_ID + ".rawlsUpsert")
            .put("isUpsert", "true")
            .put("isCWDS", "true")
            .build();

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

    // Act
    try (RecordSink recordSink = newRecordSink()) {
      recordSink.upsertBatch(
          recordType,
          /* schema= */ Map.of(), // currently ignored
          recordList,
          /* primaryKey= */ "name" // currently ignored
          );
    }

    // Assert
    try {
      // look for the blob that was created in a bucket with the appropriate json
      var blobs = storage.getBlobsInBucket();
      // confirm there is only 1
      assertThat(Iterables.size(blobs)).isEqualTo(1);
      // get the name of the blob
      blobName = blobs.iterator().next().getName();
      // check that the contents match the expected Json
      var contentStream = storage.getBlobContents(blobName);
      String contents = StreamUtils.copyToString(contentStream, StandardCharsets.UTF_8);
      assertThat(contents).isNotNull();
      return mapper.readValue(contents, new TypeReference<>() {});
    } catch (IOException e) {
      throw new RuntimeException(e);
    } finally {
      // delete the blob so the next test can have a clean slate
      if (!blobName.isEmpty()) {
        storage.deleteBlob(blobName);
      }
    }
  }

  private RawlsRecordSink newRecordSink() {
    return RawlsRecordSink.create(
        mapper,
        storage,
        pubSub,
        new ImportDetails(JOB_ID, USER_EMAIL, WORKSPACE_ID, PrefixStrategy.NONE));
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
