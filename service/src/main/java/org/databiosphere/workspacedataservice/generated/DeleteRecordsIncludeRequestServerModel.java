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
 * included record IDs array
 */

@Schema(name = "DeleteRecordsIncludeRequest", description = "included record IDs array")
@JsonTypeName("DeleteRecordsIncludeRequest")
@Generated(value = "org.openapitools.codegen.languages.SpringCodegen", comments = "Generator version: 7.8.0")
public class DeleteRecordsIncludeRequestServerModel implements DeleteRecordsByWorkspaceV1RequestServerModel {

  @Valid
  private List<String> include = new ArrayList<>();

  public DeleteRecordsIncludeRequestServerModel include(List<String> include) {
    this.include = include;
    return this;
  }

  public DeleteRecordsIncludeRequestServerModel addIncludeItem(String includeItem) {
    if (this.include == null) {
      this.include = new ArrayList<>();
    }
    this.include.add(includeItem);
    return this;
  }

  /**
   * an array of record IDs that should be deleted
   * @return include
   */
  
  @Schema(name = "include", description = "an array of record IDs that should be deleted", requiredMode = Schema.RequiredMode.NOT_REQUIRED)
  @JsonProperty("include")
  public List<String> getInclude() {
    return include;
  }

  public void setInclude(List<String> include) {
    this.include = include;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    DeleteRecordsIncludeRequestServerModel deleteRecordsIncludeRequest = (DeleteRecordsIncludeRequestServerModel) o;
    return Objects.equals(this.include, deleteRecordsIncludeRequest.include);
  }

  @Override
  public int hashCode() {
    return Objects.hash(include);
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("class DeleteRecordsIncludeRequestServerModel {\n");
    sb.append("    include: ").append(toIndentedString(include)).append("\n");
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

