package org.databiosphere.workspacedataservice.service.model;

import java.util.List;
import org.databiosphere.workspacedataservice.shared.model.RecordType;

public record RecordTypeSchema(
    RecordType name, List<AttributeSchema> attributes, int count, String primaryKey) {

  public boolean isPrimaryKey(String attribute) {
    return attribute.equals(primaryKey);
  }

  public boolean containsAttribute(String attribute) {
    return attributes.stream()
        .anyMatch(attributeSchema -> attributeSchema.name().equals(attribute));
  }
}
