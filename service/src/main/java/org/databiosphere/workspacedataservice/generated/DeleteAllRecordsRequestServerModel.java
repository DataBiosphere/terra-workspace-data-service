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
 * request body to delete all records, optionally excluding some by ID
 */

@Schema(name = "DeleteAllRecordsRequest", description = "request body to delete all records, optionally excluding some by ID")
@JsonTypeName("DeleteAllRecordsRequest")
@Generated(value = "org.openapitools.codegen.languages.SpringCodegen", comments = "Generator version: 7.8.0")
public class DeleteAllRecordsRequestServerModel {

  @Valid
  private List<String> exclude = new ArrayList<>();

  public DeleteAllRecordsRequestServerModel exclude(List<String> exclude) {
    this.exclude = exclude;
    return this;
  }

  public DeleteAllRecordsRequestServerModel addExcludeItem(String excludeItem) {
    if (this.exclude == null) {
      this.exclude = new ArrayList<>();
    }
    this.exclude.add(excludeItem);
    return this;
  }

  /**
   * delete all records *except* those specified by these record IDs
   * @return exclude
   */
  
  @Schema(name = "exclude", description = "delete all records *except* those specified by these record IDs", requiredMode = Schema.RequiredMode.NOT_REQUIRED)
  @JsonProperty("exclude")
  public List<String> getExclude() {
    return exclude;
  }

  public void setExclude(List<String> exclude) {
    this.exclude = exclude;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    DeleteAllRecordsRequestServerModel deleteAllRecordsRequest = (DeleteAllRecordsRequestServerModel) o;
    return Objects.equals(this.exclude, deleteAllRecordsRequest.exclude);
  }

  @Override
  public int hashCode() {
    return Objects.hash(exclude);
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("class DeleteAllRecordsRequestServerModel {\n");
    sb.append("    exclude: ").append(toIndentedString(exclude)).append("\n");
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
