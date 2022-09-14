package org.databiosphere.workspacedataservice.shared.model;

import com.fasterxml.jackson.annotation.JsonGetter;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;

@JsonInclude(JsonInclude.Include.NON_NULL)
public class Record {

	private RecordId id;

	private RecordType recordType;

	private RecordAttributes attributes;

	public Record(RecordId id, RecordType recordType, RecordAttributes attributes) {
		this.id = id;
		this.recordType = recordType;
		this.attributes = attributes;
	}

	public Record() {
	}

	public Record(RecordId id) {
		this.id = id;
	}

	public Record(RecordId id, RecordType type, RecordRequest request) {
		this.id = id;
		this.recordType = type;
		this.attributes = request.recordAttributes();
	}

	public RecordId getId() {
		return id;
	}

	public void setId(RecordId id) {
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
		return recordType.toPathSegment();
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
