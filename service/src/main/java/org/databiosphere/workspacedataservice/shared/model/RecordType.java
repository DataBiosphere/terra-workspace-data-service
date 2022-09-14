package org.databiosphere.workspacedataservice.shared.model;

import com.fasterxml.jackson.annotation.*;
import org.databiosphere.workspacedataservice.service.model.exception.InvalidNameException;

import java.util.Objects;

import static org.databiosphere.workspacedataservice.service.model.ReservedNames.RESERVED_NAME_PREFIX;

public class RecordType {

	private final String name;

	@JsonCreator
	public RecordType(String name) {
		this.name = name;
	}

	public static RecordType forUnitTest(String recordTypeName) {
		return new RecordType(recordTypeName);
	}

	public static RecordType fromSqlTableName(String tableName) {
		return new RecordType(tableName);
	}

	public static RecordType fromUriSegment(String uriSegment) {
		return new RecordType(uriSegment);
	}

	public String toSqlTableName() {
		return name;
	}

	@JsonValue
	public String toJsonValue() {
		return name;
	}

	public String toUriSegment() {
		return name;
	}

	public void validate() {
		if (name.startsWith(RESERVED_NAME_PREFIX)) {
			throw new InvalidNameException("Record type");
		}
	}

	/* returning the raw string value allows RecordType to be used directly as an argument to
		url templates, e.g. within unit tests
	 */
	@Override
	public String toString() {
		return name;
	}

	@Override
	public boolean equals(Object o) {
		if (this == o)
			return true;
		if (o == null || getClass() != o.getClass())
			return false;
		RecordType that = (RecordType) o;
		return Objects.equals(name, that.name);
	}

	@Override
	public int hashCode() {
		return Objects.hash(name);
	}
}
