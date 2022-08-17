package org.databiosphere.workspacedataservice.shared.model;

import com.fasterxml.jackson.annotation.JsonGetter;
import com.fasterxml.jackson.annotation.JsonInclude;

@JsonInclude(JsonInclude.Include.NON_NULL)
public class Record {

  private RecordId name;

  private RecordType recordType;

  private RecordAttributes attributes;

  public Record(RecordId name, RecordType recordType, RecordAttributes attributes) {
    this.name = name;
    this.recordType = recordType;
    this.attributes = attributes;
  }

  public Record() {}

  public Record(RecordId recordName) {
    this.name = recordName;
  }

  public Record(RecordRequest request) {
    this.name = request.recordId();
    this.recordType = request.recordType();
    this.attributes = request.recordAttributes();
  }

  public RecordId getName() {
    return name;
  }

  public void setName(RecordId name) {
    this.name = name;
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
    if (this == o) return true;
    if (!(o instanceof Record record)) return false;

    if (!getName().equals(record.getName())) return false;
    return getRecordType().equals(record.getRecordType());
  }

  @Override
  public int hashCode() {
    int result = getName().hashCode();
    result = 31 * result + getRecordType().hashCode();
    return result;
  }
}
