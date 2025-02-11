package org.databiosphere.workspacedataservice.drshub;

import com.fasterxml.jackson.annotation.JsonProperty;
import java.net.URI;
import java.util.Map;

public record ResourceMetadataResponse(@JsonProperty("accessUrl") AccessUrl accessUrl) {

  public record AccessUrl(
      @JsonProperty("url") URI url, @JsonProperty("headers") Map<String, String> headers) {}
}
