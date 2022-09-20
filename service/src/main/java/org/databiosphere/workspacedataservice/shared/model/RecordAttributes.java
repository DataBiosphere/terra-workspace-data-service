package org.databiosphere.workspacedataservice.shared.model;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;

import java.util.*;

public class RecordAttributes {

	// ========== members and constructors

	// TODO: want to use Guava ImmutableMap, or even Java unmodifiable maps, but they don't allow null values
	// use sorted map internally so getAttributes is automatically sorted
	final private TreeMap<String, Object> attributes;


	@JsonCreator
	public RecordAttributes(Map<String, Object> attributes) {
		this.attributes = new TreeMap<>(attributes);
	}

	public static RecordAttributes empty() {
		return new RecordAttributes(Collections.emptyMap());
	}

	// TODO: assess how this is used
	// ========== getAttributes

	// when serializing to json, sort attribute keys
	@JsonValue
	public Map<String, Object> getAttributes() {
		return attributes;
	}

	// ========== accessors
	/**
	 * Retrieve the value for a single named attribute
	 * 
	 * @param attributeName
	 *            name of the attribute to retrieve
	 * @return the value of the named attribute
	 */
	public Object get(String attributeName) {
		return this.attributes.get(attributeName);
	}

	public Set<Map.Entry<String, Object>> entrySet() {
		return this.attributes.entrySet();
	}

	// ========== mutators

	public RecordAttributes putAll(Map<String, Object> incoming) {
		this.attributes.putAll(incoming);
		return this;
	}

	public RecordAttributes putAll(RecordAttributes incoming) {
		return putAll(incoming.getAttributes());
	}

	public void put(String key, Object value)  {
		this.attributes.put(key, value);
	}

	public void putIfAbsent(String key, Object value) {
		this.attributes.putIfAbsent(key, value);
	}

	// ========== util methods

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
