package org.databiosphere.workspacedataservice.shared.model;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;
import java.util.AbstractMap;
import java.util.Collections;
import java.util.Comparator;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.TreeMap;
import java.util.stream.Collectors;
import org.databiosphere.workspacedataservice.shared.model.attributes.Attribute;

/** Represents the attributes of a Record. */
public class RecordAttributes {

  // internal representation is a sorted map, so json serialization
  // is nicely sorted without additional work upon render
  private final TreeMap<String, Attribute> attributes;

  // want to use Guava ImmutableMap, or even Java unmodifiable maps, but they
  // don't allow null values
  // and nulls are necessary since we may have them in the db

  // ========== members and constructors

  /**
   * Generic constructor
   *
   * @param attributes the map of attribute names->values to use for this RecordAttributes.
   */
  @JsonCreator
  public RecordAttributes(Map<String, Object> attributes) {
    // AJ-858: post-process the attributes map to look for JsonAttribute, dates, etc.
    this.attributes = new TreeMap<>(toAttributes(attributes));
  }

  /**
   * @param attributes the map of attribute names->values to use for this RecordAttributes.
   * @param primaryKeyAttributeName name of the attribute used as the primary key for the associated
   *     record's record type.
   */
  public RecordAttributes(Map<String, Object> attributes, String primaryKeyAttributeName) {
    AttributeComparator comparator = new AttributeComparator(primaryKeyAttributeName);
    this.attributes = new TreeMap<>(comparator);
    this.attributes.putAll(toAttributes(attributes));
  }

  private static Map<String, Attribute> toAttributes(Map<String, Object> input) {
    return input.entrySet().stream()
        .map(e -> new AbstractMap.SimpleEntry<>(e.getKey(), Attribute.create(e.getValue())))
        .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
  }

  /**
   * creates a RecordAttributes with no keys/values
   *
   * @return the empty RecordAttributes object
   */
  public static RecordAttributes empty() {
    return new RecordAttributes(Collections.emptyMap());
  }

  /**
   * creates a RecordAttributes with no keys/values
   *
   * @param primaryKeyAttributeName name of the attribute used as the primary key for the associated
   *     record's record type.
   * @return the empty RecordAttributes object
   */
  public static RecordAttributes empty(String primaryKeyAttributeName) {
    return new RecordAttributes(Collections.emptyMap(), primaryKeyAttributeName);
  }

  // ========== accessors

  // when serializing to json, delegate to the internal map.
  // this is private so callers aren't tempted to use it
  @SuppressWarnings("unused") // used by JsonValue but nothing else, intentionally
  @JsonValue
  private Map<String, Object> getAttributes() {
    Comparator<? super String> comparator = this.attributes.comparator();
    TreeMap<String, Object> rawValueMap = new TreeMap<>(comparator);
    // extract the underlying values from each Attribute
    this.attributes.forEach((key, value) -> rawValueMap.put(key, value.getValue()));
    return rawValueMap;
  }

  /**
   * Retrieve the value for a single named attribute
   *
   * @param attributeName name of the attribute to retrieve
   * @return the value of the named attribute
   */
  public Object getAttributeValue(String attributeName) {
    return getAttributes().get(attributeName);
  }

  public Attribute getAttribute(String attributeName) {
    return this.attributes.get(attributeName);
  }

  public boolean containsAttribute(String attributeName) {
    return this.attributes.containsKey(attributeName);
  }

  public Set<Map.Entry<String, Object>> attributeSet() {
    return getAttributes().entrySet();
  }

  // ========== mutators

  public RecordAttributes putAll(RecordAttributes incoming) {
    this.attributes.putAll(incoming.attributes);
    return this;
  }

  public RecordAttributes putAttribute(String key, Object value) {
    this.attributes.put(key, Attribute.create(value));
    return this;
  }

  public RecordAttributes putAttributeIfAbsent(String key, Object value) {
    this.attributes.putIfAbsent(key, Attribute.create(value));
    return this;
  }

  public RecordAttributes removeAttribute(String key) {
    this.attributes.remove(key);
    return this;
  }

  public RecordAttributes removeNullHeaders() {
    // There can only be one unique key so remove it.
    attributes.remove("");
    return this;
  }

  // ========== utils

  @Override
  public String toString() {
    return "RecordAttributes{" + "attributes=" + attributes + '}';
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    RecordAttributes that = (RecordAttributes) o;
    return Objects.equals(attributes, that.attributes);
  }

  @Override
  public int hashCode() {
    return Objects.hash(attributes);
  }
}
