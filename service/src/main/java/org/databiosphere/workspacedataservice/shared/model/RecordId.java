package org.databiosphere.workspacedataservice.shared.model;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;
import java.util.Objects;

public class RecordId {

	private String recordIdentifier;

	@JsonCreator
	public RecordId(String recordIdentifier) {
		this.recordIdentifier = recordIdentifier;
	}

	@JsonValue
	public String getRecordIdentifier() {
		return recordIdentifier;
	}

	@Override
	public String toString() {
		return "RecordId{" + "recordIdentifier='" + recordIdentifier + '\'' + '}';
	}

	@Override
	public boolean equals(Object o) {
		if (this == o)
			return true;
		if (o == null || getClass() != o.getClass())
			return false;
		RecordId recordId = (RecordId) o;
		return Objects.equals(recordIdentifier, recordId.recordIdentifier);
	}

	@Override
	public int hashCode() {
		return Objects.hash(recordIdentifier);
	}
}
