package org.databiosphere.workspacedataservice.service.model;

import java.util.Collections;
import java.util.Set;

public record RelationCollection(Set<Relation> relations, Set<Relation> relationArrays) {
  public static RelationCollection empty() {
    return new RelationCollection(Collections.emptySet(), Collections.emptySet());
  }
}
