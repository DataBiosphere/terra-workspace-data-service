package org.databiosphere.workspacedataservice.service.model;

import java.util.List;

public record RecordTypeSchema(String name, List<AttributeSchema> attributes, int count) {
	public RecordTypeSchema(String name, List<AttributeSchema> attributes) {
		this(name, attributes, 0);
	}
}
