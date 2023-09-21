package org.databiosphere.workspacedataservice.shared.model;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

public class BatchOperation {

  private final Record rcd;

  private final OperationType operation;

  @JsonCreator
  public BatchOperation(
      @JsonProperty("record") Record rcd, @JsonProperty("operation") OperationType operation) {
    this.rcd = rcd;
    this.operation = operation;
  }

  public Record getRecord() {
    return rcd;
  }

  public OperationType getOperation() {
    return operation;
  }
}
