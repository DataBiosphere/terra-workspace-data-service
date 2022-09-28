package org.databiosphere.workspacedataservice.shared.model;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

public class BatchOperation {

	private Record record;

	private OperationType operation;

	@JsonCreator
	public BatchOperation(@JsonProperty("record") Record record, @JsonProperty("operation") OperationType operation) {
		this.record = record;
		this.operation = operation;
	}

	public Record getRecord() {
		return record;
	}

	public OperationType getOperation() {
		return operation;
	}
}
