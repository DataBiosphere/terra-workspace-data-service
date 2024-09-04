package org.databiosphere.workspacedataservice.recordsink;

import static com.google.common.collect.MoreCollectors.onlyElement;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Iterables;
import java.io.IOException;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.function.Supplier;
import org.databiosphere.workspacedataservice.common.DataPlaneTestBase;
import org.databiosphere.workspacedataservice.dataimport.ImportDetails;
import org.databiosphere.workspacedataservice.dataimport.rawlsjson.RawlsJsonImportOptions;
import org.databiosphere.workspacedataservice.dataimport.rawlsjson.RawlsJsonJobInput;
import org.databiosphere.workspacedataservice.pubsub.PubSub;
import org.databiosphere.workspacedataservice.recordsink.RawlsAttributePrefixer.PrefixStrategy;
import org.databiosphere.workspacedataservice.recordsink.RawlsModel.AddUpdateAttribute;
import org.databiosphere.workspacedataservice.recordsink.RawlsModel.Entity;
import org.databiosphere.workspacedataservice.shared.model.CollectionId;
import org.databiosphere.workspacedataservice.shared.model.Record;
import org.databiosphere.workspacedataservice.shared.model.RecordAttributes;
import org.databiosphere.workspacedataservice.shared.model.RecordType;
import org.databiosphere.workspacedataservice.shared.model.WorkspaceId;
import org.databiosphere.workspacedataservice.storage.GcsStorage;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.util.StreamUtils;

/**
 * Smoke-tests a few paths of attribute prefixing behavior via a full RawlsRecordSink. More coverage
 * of attribute prefixing behavior is in {@link RawlsAttributePrefixerTest}
 *
 * @see RawlsAttributePrefixerTest
 */
@DirtiesContext
@SpringBootTest
@ActiveProfiles(value = "control-plane", inheritProfiles = false)
class RawlsRecordSinkPrefixingTest extends DataPlaneTestBase {
  @Autowired private ObjectMapper mapper;
  @MockBean private PubSub pubSub;

  @Qualifier("mockGcsStorage")
  @Autowired
  private GcsStorage storage;

  private final UUID WORKSPACE_ID = UUID.randomUUID();
  private final UUID JOB_ID = UUID.randomUUID();

  // ===== PFB tests =====
  @Test
  void pfbNameAttr() {
    assertPrefixingBehavior(
        "name", RecordType.valueOf("widget"), PrefixStrategy.PFB, "pfb:widget_name");
  }

  @Test
  void pfbEntityTypeAttr() {
    assertPrefixingBehavior(
        "entityType", RecordType.valueOf("widget"), PrefixStrategy.PFB, "pfb:entityType");
  }

  @Test
  void pfbIdAttr() {
    assertPrefixingBehavior(
        "widget_id", RecordType.valueOf("widget"), PrefixStrategy.PFB, "pfb:widget_id");
  }

  @Test
  void pfbNonNameAttr() {
    assertPrefixingBehavior(
        "someAttr", RecordType.valueOf("widget"), PrefixStrategy.PFB, "pfb:someAttr");
  }

  // ===== TDR tests =====
  /* TDR:
      - if input is "name", return "tdr:name"
     - if input is "entityType", return "tdr:entityType"
     - if input is "${entityType}_id", return "tdr:${entityType}_id"
     - else, return input
  */
  @Test
  void tdrNameAttr() {
    assertPrefixingBehavior("name", RecordType.valueOf("widget"), PrefixStrategy.TDR, "tdr:name");
  }

  @Test
  void tdrEntityTypeAttr() {
    assertPrefixingBehavior(
        "entityType", RecordType.valueOf("widget"), PrefixStrategy.TDR, "tdr:entityType");
  }

  @Test
  void tdrIdAttr() {
    assertPrefixingBehavior(
        "widget_id", RecordType.valueOf("widget"), PrefixStrategy.TDR, "tdr:widget_id");
  }

  @Test
  void tdrNonNameAttr() {
    assertPrefixingBehavior(
        "someAttr", RecordType.valueOf("widget"), PrefixStrategy.TDR, "someAttr");
  }

  // ===== NONE (no-prefixing) tests =====

  @Test
  void noPrefixNameAttr() {
    assertPrefixingBehavior("name", RecordType.valueOf("widget"), PrefixStrategy.NONE, "name");
  }

  @Test
  void noPrefixEntityTypeAttr() {
    assertPrefixingBehavior(
        "entityType", RecordType.valueOf("widget"), PrefixStrategy.NONE, "entityType");
  }

  @Test
  void noPrefixIdAttr() {
    assertPrefixingBehavior(
        "widget_id", RecordType.valueOf("widget"), PrefixStrategy.NONE, "widget_id");
  }

  @Test
  void noPrefixNonNameAttr() {
    assertPrefixingBehavior(
        "someAttr", RecordType.valueOf("widget"), PrefixStrategy.NONE, "someAttr");
  }

  private void assertPrefixingBehavior(
      String inputName, RecordType inputType, PrefixStrategy prefixStrategy, String expected) {

    var entities =
        doUpsert(
            new Record("id", inputType, new RecordAttributes(Map.of(inputName, "some-value"))),
            prefixStrategy);

    var operation = assertSingleOperation(AddUpdateAttribute.class, entities);
    assertEquals(expected, operation.attributeName());
  }

  private List<Entity> doUpsert(Record record, PrefixStrategy prefixStrategy) {
    Supplier<String> USER_EMAIL = () -> "userEmail";
    var recordList = List.of(record);
    var recordType = recordList.stream().map(Record::getRecordType).collect(onlyElement());
    var blobName = "";
    var importJobInput =
        new RawlsJsonJobInput(
            URI.create("gs://test-bucket/rawls-import.json"), new RawlsJsonImportOptions(false));

    // RecordSink gets its own try block because we need close() to be called before moving to the
    // assert part of the test.

    // Act
    try (RecordSink recordSink =
        RawlsRecordSink.create(
            mapper,
            storage,
            pubSub,
            new ImportDetails(
                JOB_ID,
                USER_EMAIL,
                WorkspaceId.of(WORKSPACE_ID),
                CollectionId.of(WORKSPACE_ID),
                prefixStrategy,
                importJobInput))) {

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
}
