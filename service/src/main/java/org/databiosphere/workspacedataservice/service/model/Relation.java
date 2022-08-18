package org.databiosphere.workspacedataservice.service.model;

import org.databiosphere.workspacedataservice.shared.model.RecordType;

public record Relation(String relationColName, RecordType relationRecordType) {

	@Override
	public boolean equals(Object o) {
		if (this == o)
			return true;
		if (!(o instanceof Relation that))
			return false;

		if (!relationColName().equals(that.relationColName()))
			return false;
		return relationRecordType().equals(that.relationRecordType());
	}

	@Override
	public int hashCode() {
		int result = relationColName().hashCode();
		result = 31 * result + relationRecordType().hashCode();
		return result;
	}
}
