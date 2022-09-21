package org.databiosphere.workspacedataservice.shared.model;

import com.fasterxml.jackson.annotation.JsonGetter;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Map;
import java.util.Set;

@JsonInclude(JsonInclude.Include.NON_NULL)
public class Record {

	private String id;

	@JsonProperty("type")
	private RecordType recordType;

	private RecordAttributes attributes;

	public Record(String id, RecordType recordType, RecordAttributes attributes) {
		this.id = id;
		this.recordType = recordType;
		this.attributes = attributes;
	}

	public Record() {
	}

	public Record(String id) {
		this.id = id;
	}

	public Record(String id, RecordType type, RecordRequest request) {
		this.id = id;
		this.recordType = type;
		this.attributes = request.recordAttributes();
	}

	public String getId() {
		return id;
	}

	public void setId(String id) {
		this.id = id;
	}

	public RecordAttributes getAttributes() {
		return attributes;
	}

	public void setAttributes(RecordAttributes attributes) {
		this.attributes = attributes;
	}

	// convenience methods for attribute manipulation
	public Object getAttributeValue(String attributeName) {
		return this.attributes.getAttributeValue(attributeName);
	}

	public Set<Map.Entry<String, Object>> attributeSet() {
		return this.attributes.attributeSet();
	}

	public Record putAllAttributes(RecordAttributes incoming) {
		this.attributes.putAll(incoming);
		return this;
	}

//	public Record putAll(RecordAttributes incoming) {
//		this.attributes.putAll(incoming);
//	}
//
//	public Record putAttribute(String key, Object value)  {
//		this.attributes.putAttribute(key, value);
//	}
//
//	public Record putAttributeIfAbsent(String key, Object value) {
//		this.attributes.putAttributeIfAbsent(key, value);
//		return this;
//	}


	public RecordType getRecordType() {
		return recordType;
	}

	@JsonGetter("recordType")
	public String getRecordTypeName() {
		return recordType.getName();
	}

	@Override
	public boolean equals(Object o) {
		if (this == o)
			return true;
		if (!(o instanceof Record record))
			return false;

		if (!getId().equals(record.getId()))
			return false;
		return getRecordType().equals(record.getRecordType());
	}

	@Override
	public int hashCode() {
		int result = getId().hashCode();
		result = 31 * result + getRecordType().hashCode();
		return result;
	}
}
