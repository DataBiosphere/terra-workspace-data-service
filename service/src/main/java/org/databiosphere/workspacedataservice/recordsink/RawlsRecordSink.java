package org.databiosphere.workspacedataservice.recordsink;


import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.cloud.spring.storage.GoogleStorageResource;
import com.google.cloud.storage.Blob;
import com.google.common.collect.ImmutableMap;
import com.google.mu.util.stream.BiStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import java.util.stream.Stream;
import org.databiosphere.workspacedataservice.dataimport.ImportDetails;
import org.databiosphere.workspacedataservice.pubsub.PubSub;
import org.databiosphere.workspacedataservice.recordsink.RawlsModel.AddListMember;
import org.databiosphere.workspacedataservice.recordsink.RawlsModel.AddUpdateAttribute;
import org.databiosphere.workspacedataservice.recordsink.RawlsModel.AttributeOperation;
import org.databiosphere.workspacedataservice.recordsink.RawlsModel.CreateAttributeValueList;
import org.databiosphere.workspacedataservice.recordsink.RawlsModel.Entity;
import org.databiosphere.workspacedataservice.recordsink.RawlsModel.RemoveAttribute;
import org.databiosphere.workspacedataservice.service.model.DataTypeMapping;
import org.databiosphere.workspacedataservice.service.model.exception.DataImportException;
import org.databiosphere.workspacedataservice.shared.model.Record;
import org.databiosphere.workspacedataservice.shared.model.RecordType;
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
  private final PubSub pubSub;
  private final ImportDetails importDetails;

  private final JsonWriter jsonWriter;
  private final Blob blob;

  private static final Logger LOGGER = LoggerFactory.getLogger(RawlsRecordSink.class);

  RawlsRecordSink(
      RawlsAttributePrefixer attributePrefixer,
      JsonWriter jsonWriter,
      PubSub pubSub,
      ImportDetails importDetails,
      Blob blob) {
    this.attributePrefixer = attributePrefixer;
    this.jsonWriter = jsonWriter;

    // TODO: These three instance variables are used exclusively for pubsub concerns; if we can
    //   factor the pubsub responsibility out somehow, RawlsRecordSink can be a lot more focused.
    this.pubSub = pubSub;
    this.importDetails = importDetails;
    this.blob = blob;
  }

  /**
   * Creates a {@link RawlsRecordSink} that writes a stream of JSON entities to the given {@link
   * GcsStorage}.
   *
   * @param mapper used to generate the stream of JSON entities
   * @param storage where the JSON stream will be written
   * @param pubSub to notify upon JSON stream completion
   * @param importDetails to use for generating the message to send to PubSub
   */
  public static RawlsRecordSink create(
      ObjectMapper mapper, GcsStorage storage, PubSub pubSub, ImportDetails importDetails) {
    String blobName = getBlobName(importDetails.jobId());
    Blob blob = storage.createBlob(blobName);
    return new RawlsRecordSink(
        new RawlsAttributePrefixer(importDetails.prefixStrategy()),
        JsonWriter.create(storage.getOutputStream(blob), mapper),
        pubSub,
        importDetails,
        blob);
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
  public void close() {
    jsonWriter.close();
    publishToPubSub(importDetails);
  }

  private Entity toEntity(Record record) {
    return new Entity(record.getId(), record.getRecordType().toString(), makeOperations(record));
  }

  private List<? extends AttributeOperation> makeOperations(Record record) {
    return BiStream.from(record.getAttributes().attributeSet())
        .mapKeys(
            attributeName ->
                attributePrefixer.prefix(attributeName, record.getRecordType().getName()))
        .filterValues(Objects::nonNull)
        .flatMapToObj(this::toOperations)
        .toList();
  }

  private Stream<? extends AttributeOperation> toOperations(String name, Object attributeValue) {
    if (attributeValue instanceof List<?> values) {
      return Stream.concat(
          Stream.of(new RemoveAttribute(name), new CreateAttributeValueList(name)),
          values.stream().map(value -> new AddListMember(name, value)));
    }

    return Stream.of(new AddUpdateAttribute(name, attributeValue));
  }

  static String getBlobName(UUID jobId) {
    return "%s.rawlsUpsert".formatted(jobId.toString());
  }

  private void publishToPubSub(ImportDetails importDetails) {
    UUID jobId = importDetails.jobId();
    UUID workspaceId = importDetails.collectionId();
    String user = importDetails.userEmailSupplier().get();
    Map<String, String> message =
        new ImmutableMap.Builder<String, String>()
            .put("workspaceId", workspaceId.toString())
            .put("userEmail", user)
            .put("jobId", jobId.toString())
            .put("upsertFile", blob.getBucket() + "/" + blob.getName())
            .put("isUpsert", "true")
            .put("isCWDS", "true")
            .build();
    LOGGER.info("Publishing message to pub/sub for job {} ...", jobId);
    String publishResult = pubSub.publishSync(message);
    LOGGER.info("Pub/sub publishing complete for job {}: {}", jobId, publishResult);
  }

  /**
   * {@link JsonWriter} is responsible for writing JSON to a {@link GoogleStorageResource} and is
   * intended to encapsulate the logic to get an {@link OutputStream} set up to do that and ensure
   * the array of entities generated is properly initialized and terminated with "[" tokens.
   */
  private static class JsonWriter implements AutoCloseable {
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
    private static JsonWriter create(OutputStream outputStream, ObjectMapper objectMapper) {
      try {
        return new JsonWriter(objectMapper.getFactory().createGenerator(outputStream));
      } catch (IOException e) {
        throw new JsonWriteException("Failed to create", e);
      }
    }

    /** Write a single entity to the JSON array in Google storage. */
    private void writeEntity(Entity entity) {
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
