package org.databiosphere.workspacedataservice.service;

import com.google.common.annotations.VisibleForTesting;
import com.google.mu.util.stream.BiStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import java.util.stream.Stream;
import org.databiosphere.workspacedataservice.rawls.Model;
import org.databiosphere.workspacedataservice.rawls.Model.AddListMember;
import org.databiosphere.workspacedataservice.rawls.Model.AddUpdateAttribute;
import org.databiosphere.workspacedataservice.rawls.Model.AttributeOperation;
import org.databiosphere.workspacedataservice.rawls.Model.CreateAttributeValueList;
import org.databiosphere.workspacedataservice.rawls.Model.Entity;
import org.databiosphere.workspacedataservice.rawls.Model.RemoveAttribute;
import org.databiosphere.workspacedataservice.service.BatchWriteService.RecordSink;
import org.databiosphere.workspacedataservice.service.model.DataTypeMapping;
import org.databiosphere.workspacedataservice.service.model.exception.BatchWriteException;
import org.databiosphere.workspacedataservice.shared.model.OperationType;
import org.databiosphere.workspacedataservice.shared.model.Record;
import org.databiosphere.workspacedataservice.shared.model.RecordType;

/**
 * {@link RecordSink} implementation that produces Rawls-compatible JSON using {@link Model}
 * serialization classes.
 *
 * <p>TODO(AJ-1585): integrate with storage to write JSON output to the appropriate bucket
 *
 * <p>TODO(AJ-1586): integrate with pubsub to notify Rawls when JSON is ready to be processed
 */
public class RawlsRecordSink implements RecordSink {

  private final List<Entity> entities;
  private final String attributePrefix;

  RawlsRecordSink(String attributePrefix) {
    this.attributePrefix = attributePrefix;
    // TODO: make stateless/threadsafe
    this.entities = new ArrayList<>();
  }

  public static RawlsRecordSink withPrefix(String prefix) {
    return new RawlsRecordSink(prefix);
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
      throws BatchWriteException {
    // TODO: this method signature has a lot of unused arguments, can the interface be tidied up to
    //  not require all these?
    records.stream().map(this::toEntity).forEach(entities::add);
  }

  private Entity toEntity(Record record) {
    return new Entity(record.getId(), record.getRecordType().toString(), makeOperations(record));
  }

  private List<? extends AttributeOperation> makeOperations(Record record) {
    return BiStream.from(record.getAttributes().attributeSet())
        .mapKeys(name -> getAttributeName(record.getRecordType(), name))
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

  @VisibleForTesting
  String getAttributeName(RecordType recordType, String name) {
    if (name.equals("name")) {
      return String.format("%s:%s_name", attributePrefix, recordType);
    }

    return String.format("%s:%s", attributePrefix, name);
  }

  @VisibleForTesting
  public List<Entity> getRecordedEntities() {
    return entities;
  }
}
