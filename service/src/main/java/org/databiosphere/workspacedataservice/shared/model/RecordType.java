package org.databiosphere.workspacedataservice.shared.model;

import com.fasterxml.jackson.annotation.*;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import org.databiosphere.workspacedataservice.service.model.exception.InvalidNameException;

import java.util.Objects;

import static org.databiosphere.workspacedataservice.service.model.ReservedNames.RESERVED_NAME_PREFIX;

public class RecordType {

	public static RecordType fromSqlTableName(String tableName) {
		return new RecordType(tableName);
	}

//	public static RecordType fromPathSegment(String pathSegment) {
//		return new RecordType(pathSegment);
//	}

	@JsonCreator
	public RecordType(String name) {
		this.name = name;
	}

	private final String name;

	// public String getNameXXX() {
	// return name;
	// }
	//
//	 public void setName(String name) {
//	 this.name = name;
//	 }

	public String toSqlTableName() {
		return name;
	}

	@JsonValue
	public String toPathSegment() {
		return name;
	}

	// TODO: return void instead?
	public RecordType validate() {
		if (name.startsWith(RESERVED_NAME_PREFIX)) {
			throw new InvalidNameException("Record type");
		}
		return this;
	}

	@Override
	public String toString() {
		return name;
	}
	// public String toString() {
//		return "RecordType{" + "name='" + name + '\'' + '}';
//	}

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
