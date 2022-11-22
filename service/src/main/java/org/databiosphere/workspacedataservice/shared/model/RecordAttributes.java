package org.databiosphere.workspacedataservice.shared.model;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;

import java.text.Collator;
import java.util.*;

/**
 * Represents the attributes of a Record.
 */
public class RecordAttributes {

	// internal representation is a sorted map, so json serialization
	// is nicely sorted without additional work upon render
	private final TreeMap<String, Object> attributes;
	// want to use Guava ImmutableMap, or even Java unmodifiable maps, but they
	// don't allow null values
	// and nulls are necessary since we may have them in the db

	// ========== members and constructors

	/**
	 * Generic constructor, also used as the JsonCreator.
	 * 
	 * @param attributes
	 *            the map of attribute names->values to use for this
	 *            RecordAttributes.
	 */
	@JsonCreator
	public RecordAttributes(Map<String, Object> attributes) {
		this.attributes = new TreeMap<>(attributes);
	}

	public RecordAttributes(Map<String, Object> attributes, String primaryKey) {
		AttributeComparator comparator = new AttributeComparator(primaryKey);
		this.attributes = new TreeMap<>(comparator);
		this.attributes.putAll(attributes);
	}


	/**
	 * creates a RecordAttributes with no keys/values
	 * 
	 * @return the empty RecordAttributes object
	 */
	public static RecordAttributes empty() {
		return new RecordAttributes(Collections.emptyMap());
	}

	public static RecordAttributes empty(String primaryKeyColumn) {
		return new RecordAttributes(Collections.emptyMap(), primaryKeyColumn);
	}

	// ========== accessors

	// when serializing to json, delegate to the internal map.
	// this is private so callers aren't tempted to use it
	@SuppressWarnings("unused") // used by JsonValue but nothing else, intentionally
	@JsonValue
	private Map<String, Object> getAttributes() {
		return attributes;
	}

	/**
	 * Retrieve the value for a single named attribute
	 * 
	 * @param attributeName
	 *            name of the attribute to retrieve
	 * @return the value of the named attribute
	 */
	public Object getAttributeValue(String attributeName) {
		return this.attributes.get(attributeName);
	}

	public Set<Map.Entry<String, Object>> attributeSet() {
		return this.attributes.entrySet();
	}

	// ========== mutators

	public RecordAttributes putAll(RecordAttributes incoming) {
		this.attributes.putAll(incoming.attributes);
		return this;
	}

	public RecordAttributes putAttribute(String key, Object value) {
		this.attributes.put(key, value);
		return this;
	}

	public RecordAttributes putAttributeIfAbsent(String key, Object value) {
		this.attributes.putIfAbsent(key, value);
		return this;
	}

	// ========== utils

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
