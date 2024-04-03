package org.databiosphere.workspacedataservice.pact;

import com.google.common.collect.ImmutableMap;
import java.util.Arrays;

public class PactTestSupport {

  static final String BEARER_TOKEN = "fakebearertoken";

  // headers
  static ImmutableMap<String, String> authorization(String bearerToken) {
    return new ImmutableMap.Builder<String, String>()
        .put("Authorization", String.format("Bearer %s", bearerToken))
        .build();
  }

  static ImmutableMap<String, String> contentTypeJson() {
    // pact will automatically assume an expected Content-Type of "application/json; charset=UTF-8"
    // unless we explicitly tell it otherwise
    return new ImmutableMap.Builder<String, String>()
        .put("Content-Type", "application/json")
        .build();
  }

  static ImmutableMap<String, String> acceptJson() {
    return new ImmutableMap.Builder<String, String>().put("Accept", "application/json").build();
  }

  @SafeVarargs
  static ImmutableMap<String, String> mergeHeaders(ImmutableMap<String, String>... headerMaps) {
    return Arrays.stream(headerMaps)
        .map(map -> new ImmutableMap.Builder<String, String>().putAll(map))
        .reduce(new ImmutableMap.Builder<>(), (first, second) -> first.putAll(second.build()))
        .build();
  }

  static ImmutableMap<String, String> authorizedJsonContentTypeHeaders() {
    return mergeHeaders(authorization(BEARER_TOKEN), contentTypeJson());
  }

  static ImmutableMap<String, String> authorizedAcceptJsonHeaders() {
    return mergeHeaders(authorization(BEARER_TOKEN), acceptJson());
  }
}
