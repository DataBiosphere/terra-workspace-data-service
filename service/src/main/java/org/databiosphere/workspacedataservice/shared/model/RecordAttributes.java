package org.databiosphere.workspacedataservice.shared.model;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;
import java.util.Map;
import java.util.Objects;
import java.util.TreeMap;

public class RecordAttributes {

	private Map<String, Object> attributes;

	@JsonCreator
	public RecordAttributes(Map<String, Object> attributes) {
		this.attributes = attributes;
	}

	// when serializing to json, sort attribute keys
	@JsonValue
	public Map<String, Object> getAttributes() {
		return new TreeMap<>(attributes);
	}

	@Override
	public String toString() {
		return "RecordAttributes{" + "attributes=" + attributes + '}';
	}

	@Override
	public boolean equals(Object o) {
		if (this == o)
			return true;
		if (o == null || getClass() != o.getClass())
			return false;
		RecordAttributes that = (RecordAttributes) o;
		return Objects.equals(attributes, that.attributes);
	}

	@Override
	public int hashCode() {
		return Objects.hash(attributes);
	}
}
