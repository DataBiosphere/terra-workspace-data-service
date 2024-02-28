package org.databiosphere.workspacedataservice.recordsink;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import com.google.mu.util.stream.BiStream;
import org.databiosphere.workspacedataservice.config.ConfigurationException;
import org.databiosphere.workspacedataservice.recordsink.RawlsModel.AddListMember;
import org.databiosphere.workspacedataservice.recordsink.RawlsModel.AddUpdateAttribute;
import org.databiosphere.workspacedataservice.recordsink.RawlsModel.AttributeOperation;
import org.databiosphere.workspacedataservice.recordsink.RawlsModel.CreateAttributeValueList;
import org.databiosphere.workspacedataservice.recordsink.RawlsModel.Entity;
import org.databiosphere.workspacedataservice.recordsink.RawlsModel.RemoveAttribute;
import org.databiosphere.workspacedataservice.service.model.DataTypeMapping;
import org.databiosphere.workspacedataservice.service.model.exception.BatchWriteException;
import org.databiosphere.workspacedataservice.shared.model.Record;
import org.databiosphere.workspacedataservice.shared.model.RecordType;
import org.databiosphere.workspacedataservice.storage.GcsStorage;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Consumer;
import java.util.stream.Stream;

/**
 * {@link RecordSink} implementation that produces Rawls-compatible JSON using {@link RawlsModel}
 * serialization classes.
 *
 * <p>TODO(AJ-1586): integrate with pubsub to notify Rawls when JSON is ready to be processed
 */
public class RawlsRecordSink implements RecordSink {
  private final String attributePrefix;
  private final ObjectMapper mapper;
  private final GcsStorage storage;
  private final Consumer<String> jsonConsumer;

  /** Annotates a String consumer for JSON strings emitted by {@link RawlsRecordSink}. */
  @Target({ElementType.PARAMETER, ElementType.METHOD})
  @Retention(RetentionPolicy.RUNTIME)
  public @interface RawlsJsonConsumer {}

  RawlsRecordSink(
      String attributePrefix,
      ObjectMapper mapper,
      GcsStorage storage,
      @RawlsJsonConsumer Consumer<String> jsonConsumer) {
    this.attributePrefix = attributePrefix;
    this.mapper = mapper;
    this.jsonConsumer = jsonConsumer;
    this.storage = storage;
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
      ) throws BatchWriteException, IOException {
    ImmutableList.Builder<Entity> entities = ImmutableList.builder();
    records.stream().map(this::toEntity).forEach(entities::add);
    jsonConsumer.accept(mapper.writeValueAsString(entities.build()));

    // TODO(AJ-1661) - Would it make sense to propogate the JobId into here so that BlobName can be named
    // with JobId as it is today with import service?
    if (storage != null) {
      storage.createGcsFile(new ByteArrayInputStream(mapper.writeValueAsBytes(entities.build())));
    } else {
      throw new ConfigurationException("GcsStorage is null for cWDS which is unexpected.");
    }

    // AJ-1586 - the name of where in the bucket the file is inside blobName
  }

  @Override
  public void deleteBatch(RecordType recordType, List<Record> records) throws BatchWriteException {
    throw new UnsupportedOperationException("RawlsRecordSink does not support deleteBatch");
  }

  private Entity toEntity(Record record) {
    return new Entity(record.getId(), record.getRecordType().toString(), makeOperations(record));
  }

  private List<? extends AttributeOperation> makeOperations(Record record) {
    return BiStream.from(record.getAttributes().attributeSet())
        .mapKeys(attributeName -> getAttributeName(record.getRecordType(), attributeName))
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

  private String getAttributeName(RecordType recordType, String name) {
    if (name.equals("name")) {
      return String.format("%s:%s_name", attributePrefix, recordType);
    }

    return String.format("%s:%s", attributePrefix, name);
  }
}
