package org.databiosphere.workspacedataservice.recordsink;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.cloud.spring.storage.GoogleStorageResource;
import com.google.cloud.storage.Blob;
import com.google.common.annotations.VisibleForTesting;
import com.google.mu.util.stream.BiStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Stream;
import org.databiosphere.workspacedataservice.dataimport.ImportDetails;
import org.databiosphere.workspacedataservice.dataimport.rawlsjson.RawlsJsonQuartzJob;
import org.databiosphere.workspacedataservice.pubsub.PubSub;
import org.databiosphere.workspacedataservice.pubsub.RawlsJsonPublisher;
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
import org.databiosphere.workspacedataservice.service.model.exception.DataImportException;
import org.databiosphere.workspacedataservice.shared.model.Record;
import org.databiosphere.workspacedataservice.shared.model.RecordType;
import org.databiosphere.workspacedataservice.shared.model.attributes.RelationAttribute;
import org.databiosphere.workspacedataservice.storage.GcsStorage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.util.function.ThrowingConsumer;

/**
 * {@link RecordSink} implementation that produces Rawls-compatible JSON using {@link RawlsModel}
 * serialization classes.
 */
public class RawlsRecordSink implements RecordSink {
  private final RawlsAttributePrefixer attributePrefixer;

  private final JsonWriter jsonWriter;
  private final RawlsJsonPublisher publisher;

  private boolean isComplete = false;

  private static final Logger LOGGER = LoggerFactory.getLogger(RawlsRecordSink.class);

  RawlsRecordSink(
      RawlsAttributePrefixer attributePrefixer,
      JsonWriter jsonWriter,
      RawlsJsonPublisher publisher) {
    this.attributePrefixer = attributePrefixer;
    this.jsonWriter = jsonWriter;
    this.publisher = publisher;
  }

  /**
   * Creates a {@link RawlsRecordSink} that writes a stream of JSON entities to the given {@link
   * GcsStorage}.
   *
   * @param mapper used to generate the stream of JSON entities
   * @param storage where the JSON stream will be written
   * @param pubSub to notify upon JSON stream completion
   * @param details to use for generating the message to send to PubSub
   */
  public static RawlsRecordSink create(
      ObjectMapper mapper, GcsStorage storage, PubSub pubSub, ImportDetails details) {
    Blob blob = storage.createBlob(RawlsJsonQuartzJob.rawlsJsonBlobName(details.jobId()));
    return new RawlsRecordSink(
        new RawlsAttributePrefixer(details.prefixStrategy()),
        JsonWriter.create(storage.getOutputStream(blob), mapper),
        new RawlsJsonPublisher(pubSub, details, blob, /* isUpsert= */ true));
  }

  @Override
  public Map<String, DataTypeMapping> createOrModifyRecordType(
      RecordType recordType,
      Map<String, DataTypeMapping> schema,
      List<Record> records,
      String primaryKey) {
    // This is a no-op for Rawls as the schema changes occur as a side effect of writeBatch
    return schema;
  }

  @Override
  public void upsertBatch(
      RecordType recordType,
      Map<String, DataTypeMapping> schema, // ignored
      List<Record> records,
      String primaryKey // ignored
      ) {
    records.stream().map(this::toEntity).forEach(jsonWriter::writeEntity);
  }

  @Override
  public void deleteBatch(RecordType recordType, List<Record> records) {
    throw new UnsupportedOperationException("RawlsRecordSink does not support deleteBatch");
  }

  @Override
  public void success() throws DataImportException {
    isComplete = true;
    publisher.publish();
  }

  @Override
  public void close() {
    if (!isComplete) {
      LOGGER.warn(
          "RawlsRecordSink was closed before success() was called."
              + " This usually indicates an error while processing records, but could also indicate"
              + " a code path that never calls success().");
    }
    jsonWriter.close();
  }

  private Entity toEntity(Record wdsRecord) {
    return new Entity(
        wdsRecord.getId(), wdsRecord.getRecordType().toString(), makeOperations(wdsRecord));
  }

  private List<AttributeOperation> makeOperations(Record wdsRecord) {
    return BiStream.from(wdsRecord.getAttributes().attributeSet())
        .mapKeys(
            attributeName ->
                attributePrefixer.prefix(attributeName, wdsRecord.getRecordType().getName()))
        .filterValues(Objects::nonNull)
        .flatMapToObj(this::toOperations)
        .toList();
  }

  private Stream<AttributeOperation> toOperations(String name, Object attributeValue) {
    if (attributeValue instanceof List<?> values) {
      var createListCommand =
          containsRelations(values)
              ? new CreateAttributeEntityReferenceList(name)
              : new CreateAttributeValueList(name);

      return Stream.concat(
          Stream.of(new RemoveAttribute(name), createListCommand),
          values.stream()
              .map(this::maybeCoerceRelation)
              .map(value -> new AddListMember(name, value)));
    }

    return Stream.of(new AddUpdateAttribute(name, maybeCoerceRelation(attributeValue)));
  }

  private boolean containsRelations(List<?> values) {
    return values.stream()
        .filter(Objects::nonNull)
        .map(Object::getClass)
        .anyMatch(RelationAttribute.class::equals);
  }

  private AttributeValue maybeCoerceRelation(Object originalValue) {
    if (originalValue instanceof RelationAttribute relationAttribute) {
      return AttributeValue.of(EntityReference.fromRelationAttribute(relationAttribute));
    }
    return AttributeValue.of(originalValue);
  }

  /**
   * {@link JsonWriter} is responsible for writing JSON to a {@link GoogleStorageResource} and is
   * intended to encapsulate the logic to get an {@link OutputStream} set up to do that and ensure
   * the array of entities generated is properly initialized and terminated with "[" tokens.
   */
  @VisibleForTesting
  static class JsonWriter implements AutoCloseable {
    private final JsonGenerator jsonGenerator;

    // TODO: consider using a simple state machine here [enum INITIALIZED, WRITING, CLOSED] to
    //  ensure the array stream is only started and closed once
    private boolean streamStarted = false;

    private JsonWriter(JsonGenerator jsonGenerator) {
      this.jsonGenerator = jsonGenerator;
    }

    /**
     * Initialize a new {@link JsonWriter} to write to the given {@link GoogleStorageResource} using
     * the provided {@link ObjectMapper}.
     */
    @VisibleForTesting
    static JsonWriter create(OutputStream outputStream, ObjectMapper objectMapper) {
      try {
        return new JsonWriter(objectMapper.getFactory().createGenerator(outputStream));
      } catch (IOException e) {
        throw new JsonWriteException("Failed to create", e);
      }
    }

    /** Write a single entity to the JSON array in Google storage. */
    @VisibleForTesting
    void writeEntity(Entity entity) {
      if (!streamStarted) {
        writeJson(JsonGenerator::writeStartArray); // write a "[" to begin the array of entities
        streamStarted = true;
      }

      writeJson(generator -> generator.writePOJO(entity));
    }

    /** Terminate the JSON array and close the underlying {@link JsonGenerator}. */
    @Override
    public void close() {
      if (streamStarted) {
        writeJson(JsonGenerator::writeEndArray); // write a "]" to end the array of entities
      }
      try {
        jsonGenerator.close();
      } catch (IOException e) {
        throw new JsonWriteException("Failed to close", e);
      }
    }

    private void writeJson(ThrowingConsumer<JsonGenerator> jsonGeneratorConsumer) {
      jsonGeneratorConsumer.accept(jsonGenerator);
      try {
        jsonGenerator.flush();
      } catch (IOException e) {
        throw new JsonWriteException("Failed to flush", e);
      }
    }
  }

  /**
   * This unchecked exception wraps the {@link IOException} that can occur at various stages while
   * establishing a stream and writing JSON to Google Storage. It extends {@link
   * DataImportException} so callers don't need to handle it explicitly.
   */
  public static class JsonWriteException extends DataImportException {
    public JsonWriteException(String message, IOException cause) {
      super(message, cause);
    }
  }
}
