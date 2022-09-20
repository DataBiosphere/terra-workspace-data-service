package org.databiosphere.workspacedataservice.shared.model;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;
//import com.google.common.collect.ImmutableMap;
//import com.google.common.collect.ImmutableSortedMap;
//import graphql.collect.ImmutableMapWithNullValues;

import java.util.Map;
import java.util.Objects;
import java.util.TreeMap;

public class RecordAttributes {

	// ========== members and constructors

	// private ImmutableSortedMap<String, Object> attributes;
	// private ImmutableMapWithNullValues<String, Object> attributes;
	// use sorted map internally so getAttributes is automatically sorted
	private TreeMap<String, Object> attributes;


	@JsonCreator
	public RecordAttributes(Map<String, Object> attributes) {
		this.attributes = new TreeMap<>(attributes);
	}

	// TODO: assess how this is used
	// ========== getAttributes

	// when serializing to json, sort attribute keys
	@JsonValue
	public Map<String, Object> getAttributes() {
		return attributes;
	}

	// ========== accessors

	// TODO: implement iterator

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

	/**
	 * find all relation attributes
	 */
	public void getRelations() {
		// TODO
	}

	// ========== mutators

	public RecordAttributes putAll(Map<String, Object> incoming) {
		this.attributes.putAll(incoming);
		return this;
	}

	public RecordAttributes putAll(RecordAttributes incoming) {
		return putAll(incoming.getAttributes());
	}

	public RecordAttributes putIfAbsent(String key, Object value) {
		this.attributes.putIfAbsent(key, value);
		return this;
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
