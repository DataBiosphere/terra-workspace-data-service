package org.databiosphere.workspacedataservice.recordsink;

import static com.google.common.collect.MoreCollectors.onlyElement;
import static java.util.Arrays.stream;
import static java.util.Collections.emptyMap;
import static java.util.stream.Stream.concat;
import static org.assertj.core.api.Assertions.assertThat;
import static org.databiosphere.workspacedataservice.dataimport.rawlsjson.RawlsJsonQuartzJob.rawlsJsonBlobName;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import java.io.IOException;
import java.io.OutputStream;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.databiosphere.workspacedataservice.common.TestBase;
import org.databiosphere.workspacedataservice.dataimport.ImportDetails;
import org.databiosphere.workspacedataservice.dataimport.ImportJobInput;
import org.databiosphere.workspacedataservice.dataimport.rawlsjson.RawlsJsonImportOptions;
import org.databiosphere.workspacedataservice.generated.ImportRequestServerModel.TypeEnum;
import org.databiosphere.workspacedataservice.pubsub.PubSub;
import org.databiosphere.workspacedataservice.pubsub.RawlsJsonPublisher;
import org.databiosphere.workspacedataservice.recordsink.RawlsAttributePrefixer.PrefixStrategy;
import org.databiosphere.workspacedataservice.recordsink.RawlsModel.AddListMember;
import org.databiosphere.workspacedataservice.recordsink.RawlsModel.AddUpdateAttribute;
import org.databiosphere.workspacedataservice.recordsink.RawlsModel.AttributeOperation;
import org.databiosphere.workspacedataservice.recordsink.RawlsModel.AttributeValue;
import org.databiosphere.workspacedataservice.recordsink.RawlsModel.CreateAttributeEntityReferenceList;
import org.databiosphere.workspacedataservice.recordsink.RawlsModel.CreateAttributeValueList;
import org.databiosphere.workspacedataservice.recordsink.RawlsModel.Entity;
import org.databiosphere.workspacedataservice.recordsink.RawlsModel.EntityReference;
import org.databiosphere.workspacedataservice.recordsink.RawlsModel.RemoveAttribute;
import org.databiosphere.workspacedataservice.service.model.DataTypeMapping;
import org.databiosphere.workspacedataservice.shared.model.CollectionId;
import org.databiosphere.workspacedataservice.shared.model.Record;
import org.databiosphere.workspacedataservice.shared.model.RecordAttributes;
import org.databiosphere.workspacedataservice.shared.model.RecordType;
import org.databiosphere.workspacedataservice.shared.model.WorkspaceId;
import org.databiosphere.workspacedataservice.shared.model.attributes.RelationAttribute;
import org.databiosphere.workspacedataservice.storage.GcsStorage;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
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

  /** ArgumentCaptor for the message passed to {@link PubSub#publishSync(Map)}. */
  @Captor private ArgumentCaptor<Map<String, String>> pubSubMessageCaptor;

  private final UUID WORKSPACE_ID = UUID.randomUUID();
  private final UUID JOB_ID = UUID.randomUUID();
  private final Supplier<String> USER_EMAIL = () -> "userEmail";

  @Test
  void translatesEntityFields() {
    var entities = doUpsert(makeRecord(/* type= */ "widget", /* id= */ "id", emptyMap()));

    var entity = assertSingle(entities);
    assertThat(entity.name()).isEqualTo("id");
    assertThat(entity.entityType()).isEqualTo("widget");
  }

  @AfterEach
  void teardown() {
    storage.getBlobsInBucket().forEach(blob -> storage.deleteBlob(blob.getName()));
  }

  @Test
  void translatesBooleanAttribute() {
    var entities =
        doUpsert(makeRecord(/* type= */ "widget", /* id= */ "id", Map.of("booleanKey", true)));
    var operation = assertSingleOperation(AddUpdateAttribute.class, entities);
    assertThat(operation.attributeName()).isEqualTo("booleanKey");
    assertThat(operation.addUpdateAttribute().value()).isEqualTo(true);
  }

  @Test
  void translatesBooleanArrayAttribute() {
    var entities =
        doUpsert(
            makeRecord(
                /* type= */ "widget",
                /* id= */ "id",
                Map.of("booleans", List.of(true, false, true, true))));

    var entity = assertSingle(entities);
    assertThat(entity.operations())
        .containsExactlyElementsOf(
            expectedArrayCreationOperations(
                new CreateAttributeValueList("booleans"), List.of(true, false, true, true)));
  }

  @Test
  void translatesStringAttribute() {
    var entities =
        doUpsert(
            makeRecord(/* type= */ "widget", /* id= */ "id", Map.of("stringKey", "someValue")));

    var operation = assertSingleOperation(AddUpdateAttribute.class, entities);
    assertThat(operation.attributeName()).isEqualTo("stringKey");
    assertThat(operation.addUpdateAttribute().value()).isEqualTo("someValue");
  }

  @Test
  void translatesStringArrayAttribute() {
    var entities =
        doUpsert(
            makeRecord(
                /* type= */ "widget",
                /* id= */ "id",
                Map.of("strings", List.of("value1", "value2"))));

    var entity = assertSingle(entities);
    assertThat(entity.operations())
        .containsExactlyElementsOf(
            expectedArrayCreationOperations(
                new CreateAttributeValueList("strings"), List.of("value1", "value2")));
  }

  @Test
  void translatesIntegerAttribute() {
    var entities =
        doUpsert(makeRecord(/* type= */ "widget", /* id= */ "id", Map.of("integerKey", 42)));

    var operation = assertSingleOperation(AddUpdateAttribute.class, entities);
    assertThat(operation.attributeName()).isEqualTo("integerKey");
    assertThat(operation.addUpdateAttribute().value()).isEqualTo(42);
  }

  @Test
  void translatesIntegerArrayAttribute() {
    var entities =
        doUpsert(
            makeRecord(/* type= */ "widget", /* id= */ "id", Map.of("integers", List.of(1, 2, 3))));

    var entity = assertSingle(entities);
    assertThat(entity.operations())
        .containsExactlyElementsOf(
            expectedArrayCreationOperations(
                new CreateAttributeValueList("integers"), List.of(1, 2, 3)));
  }

  @Test
  void translatesFloatAttribute() {
    var entities =
        doUpsert(makeRecord(/* type= */ "widget", /* id= */ "id", Map.of("floatKey", 42.0)));

    var operation = assertSingleOperation(AddUpdateAttribute.class, entities);
    assertThat(operation.attributeName()).isEqualTo("floatKey");
    assertThat(operation.addUpdateAttribute().value()).isEqualTo(42.0);
  }

  @Test
  void translatesFloatArrayAttribute() {
    var entities =
        doUpsert(
            makeRecord(
                /* type= */ "widget", /* id= */ "id", Map.of("floats", List.of(1.0, 2.0, 3.0))));

    var entity = assertSingle(entities);
    assertThat(entity.operations())
        .containsExactlyElementsOf(
            expectedArrayCreationOperations(
                new CreateAttributeValueList("floats"), List.of(1.0, 2.0, 3.0)));
  }

  @Test
  void translatesJsonAttribute() {
    var entities =
        doUpsert(
            makeRecord(
                /* type= */ "widget",
                /* id= */ "id",
                Map.of(
                    "jsonKey",
                    new ImmutableMap.Builder<String, Object>()
                        .put("key1", "value1")
                        .put("key2", "value2")
                        .build())));

    var operation = assertSingleOperation(AddUpdateAttribute.class, entities);
    assertThat(operation.attributeName()).isEqualTo("jsonKey");
    assertThat(operation.addUpdateAttribute().value())
        .isEqualTo("{\"key1\":\"value1\",\"key2\":\"value2\"}");
  }

  @Test
  void translatesJsonArrayAttribute() {
    var entities =
        doUpsert(
            makeRecord(
                /* type= */ "widget",
                /* id= */ "id",
                Map.of(
                    "jsons",
                    List.of(
                        new ImmutableMap.Builder<String, Object>()
                            .put("key1", "value1")
                            .put("key2", "value2")
                            .build(),
                        new ImmutableMap.Builder<String, Object>()
                            .put("key3", "value3")
                            .put("key4", "value4")
                            .build()))));

    var entity = assertSingle(entities);
    assertThat(entity.operations())
        .containsExactlyElementsOf(
            expectedArrayCreationOperations(
                new CreateAttributeValueList("jsons"),
                List.of(
                    "{\"key1\":\"value1\",\"key2\":\"value2\"}",
                    "{\"key3\":\"value3\",\"key4\":\"value4\"}")));
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
  void translatesMultipleAttributes() {
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
  void translatesRelationAttribute() {
    var entities =
        doUpsert(
            makeRecord(
                /* type= */ "widget",
                /* id= */ "id",
                Map.of("relationKey", relationAttribute("widget", "widget-id"))));

    var operation = assertSingleOperation(AddUpdateAttribute.class, entities);

    assertThat(operation.attributeName()).isEqualTo("relationKey");
    assertThat(operation.addUpdateAttribute().value())
        .isEqualTo(rawlsEntityReference("widget", "widget-id"));
  }

  @Test
  void translatesRelationArrayAttribute() {
    var entities =
        doUpsert(
            makeRecord(
                /* type= */ "widget",
                /* id= */ "id",
                Map.of(
                    "widgetRelations",
                    List.of(
                        relationAttribute("widget", "widget-id1"),
                        relationAttribute("widget", "widget-id2")))));

    var entity = assertSingle(entities);
    assertThat(entity.operations())
        .containsExactlyElementsOf(
            expectedArrayCreationOperations(
                new CreateAttributeEntityReferenceList("widgetRelations"),
                List.of(
                    rawlsEntityReference("widget", "widget-id1"),
                    rawlsEntityReference("widget", "widget-id2"))));
  }

  @Test
  void batchDeleteNotSupported() {
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
            .put("userEmail", USER_EMAIL.get())
            .put("jobId", JOB_ID.toString())
            .put("upsertFile", storage.getBucketName() + "/" + rawlsJsonBlobName(JOB_ID))
            .put("isUpsert", "true")
            .put("isCWDS", "true")
            .build();

    verify(pubSub, times(1)).publishSync(pubSubMessageCaptor.capture());

    assertThat(pubSubMessageCaptor.getValue()).isEqualTo(expectedMessage);
  }

  @Disabled("AJ-1808: Don't publish to Rawls on failure.")
  @Test
  void doesNotSendPubSubOnFailure() throws IOException {
    // Arrange
    Record record = makeRecord(/* type= */ "widget", /* id= */ "id", emptyMap());

    // partially mock JsonWriter to throw an exception when writeEntity() is called
    var mockJsonWriter = spy(RawlsRecordSink.JsonWriter.create(mock(OutputStream.class), mapper));
    doAnswer(
            invocation -> {
              throw new IOException("stubbed exception");
            })
        .when(mockJsonWriter)
        .writeEntity(any(Entity.class));
    var mockJsonPublisher = mock(RawlsJsonPublisher.class);

    // Act / Assert
    try (RecordSink recordSink =
        new RawlsRecordSink(
            new RawlsAttributePrefixer(PrefixStrategy.NONE), mockJsonWriter, mockJsonPublisher)) {

      RecordType recordType = record.getRecordType();
      Map<String, DataTypeMapping> ignoredSchema = Map.of();
      List<Record> records = List.of(record);

      assertThrows(
          IOException.class,
          () -> recordSink.upsertBatch(recordType, ignoredSchema, records, "ignoredPrimaryKey"));
    }

    // Assert
    verify(mockJsonPublisher, never()).publish();
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
    ImportJobInput importJobInput =
        new ImportJobInput(
            URI.create("gs://test-bucket/rawls-import.json"),
            TypeEnum.RAWLSJSON,
            new RawlsJsonImportOptions(false));
    return RawlsRecordSink.create(
        mapper,
        storage,
        pubSub,
        new ImportDetails(
            JOB_ID,
            USER_EMAIL,
            WorkspaceId.of(WORKSPACE_ID),
            CollectionId.of(WORKSPACE_ID),
            PrefixStrategy.NONE,
            importJobInput));
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

  private Iterable<? extends AttributeOperation> expectedArrayCreationOperations(
      AttributeOperation expectedArrayCreationOperation, List<?> expectedElements) {
    var attributeName = expectedArrayCreationOperation.attributeName();
    var expectedOperationsBuilder =
        new ImmutableList.Builder<AttributeOperation>()
            .add(new RemoveAttribute(attributeName))
            .add(expectedArrayCreationOperation);
    expectedElements.forEach(
        element ->
            expectedOperationsBuilder.add(
                new AddListMember(attributeName, AttributeValue.of(element))));

    return expectedOperationsBuilder.build();
  }

  private static <T extends AttributeOperation> List<T> filterOperations(
      Class<T> type, Collection<? extends AttributeOperation> operations) {
    return operations.stream()
        .filter(type::isInstance)
        .map(type::cast)
        .collect(Collectors.toList());
  }

  private RelationAttribute relationAttribute(String type, String id) {
    return new RelationAttribute(RecordType.valueOf(type), id);
  }

  private EntityReference rawlsEntityReference(String type, String id) {
    return new EntityReference(RecordType.valueOf(type), id);
  }
}
