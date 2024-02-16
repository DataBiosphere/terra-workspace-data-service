package org.databiosphere.workspacedataservice.recordsink;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import com.google.mu.util.stream.BiStream;
import java.io.IOException;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import java.util.function.Consumer;
import java.util.stream.Stream;
import org.databiosphere.workspacedataservice.recordsink.RawlsModel.AddListMember;
import org.databiosphere.workspacedataservice.recordsink.RawlsModel.AddUpdateAttribute;
import org.databiosphere.workspacedataservice.recordsink.RawlsModel.AttributeOperation;
import org.databiosphere.workspacedataservice.recordsink.RawlsModel.CreateAttributeValueList;
import org.databiosphere.workspacedataservice.recordsink.RawlsModel.Entity;
import org.databiosphere.workspacedataservice.recordsink.RawlsModel.RemoveAttribute;
import org.databiosphere.workspacedataservice.service.model.DataTypeMapping;
import org.databiosphere.workspacedataservice.service.model.exception.BatchWriteException;
import org.databiosphere.workspacedataservice.shared.model.OperationType;
import org.databiosphere.workspacedataservice.shared.model.Record;
import org.databiosphere.workspacedataservice.shared.model.RecordType;

/**
 * {@link RecordSink} implementation that produces Rawls-compatible JSON using {@link RawlsModel}
 * serialization classes.
 *
 * <p>TODO(AJ-1585): integrate with storage to write JSON output to the appropriate bucket
 *
 * <p>TODO(AJ-1586): integrate with pubsub to notify Rawls when JSON is ready to be processed
 */
public class RawlsRecordSink implements RecordSink {
  private final String attributePrefix;
  private final ObjectMapper mapper;
  private final Consumer<String> jsonConsumer;

  /** Annotates a String consumer for JSON strings emitted by {@link RawlsRecordSink}. */
  @Target({ElementType.PARAMETER, ElementType.METHOD})
  @Retention(RetentionPolicy.RUNTIME)
  public @interface RawlsJsonConsumer {}

  RawlsRecordSink(
      String attributePrefix,
      ObjectMapper mapper,
      @RawlsJsonConsumer Consumer<String> jsonConsumer) {
    this.attributePrefix = attributePrefix;
    this.mapper = mapper;
    this.jsonConsumer = jsonConsumer;
  }

  @Override
  public Map<String, DataTypeMapping> createOrModifyRecordType(
      UUID collectionId,
      RecordType recordType,
      Map<String, DataTypeMapping> schema,
      List<Record> records,
      String recordTypePrimaryKey) {
    // This is a no-op for Rawls as the schema changes occur as a side effect of writeBatch
    return schema;
  }

  @Override
  public void writeBatch(
      UUID collectionId,
      RecordType recordType,
      Map<String, DataTypeMapping> schema,
      OperationType opType,
      List<Record> records,
      String primaryKey)
      throws BatchWriteException, IOException {
    // TODO: this method signature has a lot of unused arguments, can the interface be tidied up to
    //  not require all these?
    ImmutableList.Builder<Entity> entities = ImmutableList.builder();
    records.stream().map(this::toEntity).forEach(entities::add);
    jsonConsumer.accept(mapper.writeValueAsString(entities.build()));
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
