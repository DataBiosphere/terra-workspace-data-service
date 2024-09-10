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
 * A request body to delete records in bulk. The caller must either:  (1) specify which records should be deleted using the &#x60;record_ids&#x60; field  *OR*  (2) set &#x60;delete_all&#x3D;true&#x60;.  If &#x60;delete_all&#x3D;true&#x60;, the caller may optionally specify a list of records  to be spared from deletion using the &#x60;excluded_record_ids&#x60; field. 
 */

@Schema(name = "DeleteRecordsRequest", description = "A request body to delete records in bulk. The caller must either:  (1) specify which records should be deleted using the `record_ids` field  *OR*  (2) set `delete_all=true`.  If `delete_all=true`, the caller may optionally specify a list of records  to be spared from deletion using the `excluded_record_ids` field. ")
@JsonTypeName("DeleteRecordsRequest")
@Generated(value = "org.openapitools.codegen.languages.SpringCodegen", comments = "Generator version: 7.8.0")
public class DeleteRecordsRequestServerModel {

  @Valid
  private List<String> recordIds = new ArrayList<>();

  @Valid
  private List<String> excludedRecordIds = new ArrayList<>();

  private Boolean deleteAll = false;

  public DeleteRecordsRequestServerModel recordIds(List<String> recordIds) {
    this.recordIds = recordIds;
    return this;
  }

  public DeleteRecordsRequestServerModel addRecordIdsItem(String recordIdsItem) {
    if (this.recordIds == null) {
      this.recordIds = new ArrayList<>();
    }
    this.recordIds.add(recordIdsItem);
    return this;
  }

  /**
   * an array of record IDs that should be deleted
   * @return recordIds
   */
  
  @Schema(name = "record_ids", description = "an array of record IDs that should be deleted", requiredMode = Schema.RequiredMode.NOT_REQUIRED)
  @JsonProperty("record_ids")
  public List<String> getRecordIds() {
    return recordIds;
  }

  public void setRecordIds(List<String> recordIds) {
    this.recordIds = recordIds;
  }

  public DeleteRecordsRequestServerModel excludedRecordIds(List<String> excludedRecordIds) {
    this.excludedRecordIds = excludedRecordIds;
    return this;
  }

  public DeleteRecordsRequestServerModel addExcludedRecordIdsItem(String excludedRecordIdsItem) {
    if (this.excludedRecordIds == null) {
      this.excludedRecordIds = new ArrayList<>();
    }
    this.excludedRecordIds.add(excludedRecordIdsItem);
    return this;
  }

  /**
   * an array of record IDs that should NOT be deleted.
   * @return excludedRecordIds
   */
  
  @Schema(name = "excluded_record_ids", description = "an array of record IDs that should NOT be deleted.", requiredMode = Schema.RequiredMode.NOT_REQUIRED)
  @JsonProperty("excluded_record_ids")
  public List<String> getExcludedRecordIds() {
    return excludedRecordIds;
  }

  public void setExcludedRecordIds(List<String> excludedRecordIds) {
    this.excludedRecordIds = excludedRecordIds;
  }

  public DeleteRecordsRequestServerModel deleteAll(Boolean deleteAll) {
    this.deleteAll = deleteAll;
    return this;
  }

  /**
   * if true, deletes all records
   * @return deleteAll
   */
  
  @Schema(name = "delete_all", description = "if true, deletes all records", requiredMode = Schema.RequiredMode.NOT_REQUIRED)
  @JsonProperty("delete_all")
  public Boolean getDeleteAll() {
    return deleteAll;
  }

  public void setDeleteAll(Boolean deleteAll) {
    this.deleteAll = deleteAll;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    DeleteRecordsRequestServerModel deleteRecordsRequest = (DeleteRecordsRequestServerModel) o;
    return Objects.equals(this.recordIds, deleteRecordsRequest.recordIds) &&
        Objects.equals(this.excludedRecordIds, deleteRecordsRequest.excludedRecordIds) &&
        Objects.equals(this.deleteAll, deleteRecordsRequest.deleteAll);
  }

  @Override
  public int hashCode() {
    return Objects.hash(recordIds, excludedRecordIds, deleteAll);
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("class DeleteRecordsRequestServerModel {\n");
    sb.append("    recordIds: ").append(toIndentedString(recordIds)).append("\n");
    sb.append("    excludedRecordIds: ").append(toIndentedString(excludedRecordIds)).append("\n");
    sb.append("    deleteAll: ").append(toIndentedString(deleteAll)).append("\n");
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

