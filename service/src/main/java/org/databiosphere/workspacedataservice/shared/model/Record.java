package org.databiosphere.workspacedataservice.shared.model;

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

	public Object setAttributeValue(String attributeName, String valueToSet) {
		return this.attributes.putAttribute(attributeName, valueToSet);
	}

	public Set<Map.Entry<String, Object>> attributeSet() {
		return this.attributes.attributeSet();
	}

	public Record putAllAttributes(RecordAttributes incoming) {
		this.attributes.putAll(incoming);
		return this;
	}

	public RecordType getRecordType() {
		return recordType;
	}

	@JsonProperty("type")
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

	@Override
	public String toString() {
		return "Record{" + "id='" + id + '\'' + ", recordType=" + recordType + ", attributes=" + attributes + '}';
	}
}
