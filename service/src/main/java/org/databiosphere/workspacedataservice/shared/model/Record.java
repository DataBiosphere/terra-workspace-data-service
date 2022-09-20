package org.databiosphere.workspacedataservice.shared.model;

import com.fasterxml.jackson.annotation.JsonGetter;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;

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
