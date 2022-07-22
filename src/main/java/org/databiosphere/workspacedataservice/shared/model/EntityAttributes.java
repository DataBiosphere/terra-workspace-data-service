package org.databiosphere.workspacedataservice.shared.model;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;

import java.util.*;

public class EntityAttributes {

    private Map<String, Object> attributes;

    public EntityAttributes() {
        this.attributes = new HashMap<>();
    }

    @JsonCreator
    public EntityAttributes(Map<String, Object> attributes) {
        this.attributes = attributes;
    }

    // when serializing to json, sort attribute keys
    @JsonValue
    public Map<String, Object> asMap() {
        return new TreeMap<>(attributes);
    }

    // ========== accessors
    public void put(String key, Object value) {
        this.attributes.put(key, value);
    }
    public void putAll(EntityAttributes entityAttributes) {
        this.attributes.putAll(entityAttributes.asMap());
    }

    public void remove(String key) {
        this.attributes.remove(key);
    }

    public int size() {
        return this.attributes.size();
    }

    public Set<String> keySet() {
        return this.attributes.keySet();
    }

    public Collection<Object> values() {
        return this.attributes.values();
    }

    @Override
    public String toString() {
        return "EntityAttributes{" +
                "attributes=" + attributes +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        EntityAttributes that = (EntityAttributes) o;
        return Objects.equals(attributes, that.attributes);
    }

    @Override
    public int hashCode() {
        return Objects.hash(attributes);
    }


}
