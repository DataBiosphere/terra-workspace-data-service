package org.databiosphere.workspacedataservice.generated;

import java.net.URI;
import java.util.Objects;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonTypeName;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.openapitools.jackson.nullable.JsonNullable;
import java.time.OffsetDateTime;
import jakarta.validation.Valid;
import jakarta.validation.constraints.*;
import io.swagger.v3.oas.annotations.media.Schema;


import java.util.*;
import jakarta.annotation.Generated;

/**
 * a required deletion specification
 */

@Schema(name = "DeleteRecordsResponse", description = "a required deletion specification")
@JsonTypeName("DeleteRecordsResponse")
@Generated(value = "org.openapitools.codegen.languages.SpringCodegen", comments = "Generator version: 7.8.0")
public class DeleteRecordsResponseServerModel {

  @Valid
  private List<String> deletedRecords = new ArrayList<>();

  public DeleteRecordsResponseServerModel deletedRecords(List<String> deletedRecords) {
    this.deletedRecords = deletedRecords;
    return this;
  }

  public DeleteRecordsResponseServerModel addDeletedRecordsItem(String deletedRecordsItem) {
    if (this.deletedRecords == null) {
      this.deletedRecords = new ArrayList<>();
    }
    this.deletedRecords.add(deletedRecordsItem);
    return this;
  }

  /**
   * an array of record IDs that have been deleted
   * @return deletedRecords
   */
  
  @Schema(name = "deletedRecords", description = "an array of record IDs that have been deleted", requiredMode = Schema.RequiredMode.NOT_REQUIRED)
  @JsonProperty("deletedRecords")
  public List<String> getDeletedRecords() {
    return deletedRecords;
  }

  public void setDeletedRecords(List<String> deletedRecords) {
    this.deletedRecords = deletedRecords;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    DeleteRecordsResponseServerModel deleteRecordsResponse = (DeleteRecordsResponseServerModel) o;
    return Objects.equals(this.deletedRecords, deleteRecordsResponse.deletedRecords);
  }

  @Override
  public int hashCode() {
    return Objects.hash(deletedRecords);
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("class DeleteRecordsResponseServerModel {\n");
    sb.append("    deletedRecords: ").append(toIndentedString(deletedRecords)).append("\n");
    sb.append("}");
    return sb.toString();
  }

  /**
   * Convert the given object to string with each line indented by 4 spaces
   * (except the first line).
   */
  private String toIndentedString(Object o) {
    if (o == null) {
      return "null";
    }
    return o.toString().replace("\n", "\n    ");
  }
}

