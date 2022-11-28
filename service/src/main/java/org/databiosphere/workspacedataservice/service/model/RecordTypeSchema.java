package org.databiosphere.workspacedataservice.service.model;

import org.databiosphere.workspacedataservice.shared.model.RecordType;

import java.util.List;

public record RecordTypeSchema(RecordType name, List<AttributeSchema> attributes, int count, String uniqueRowIdentifier) {
}
