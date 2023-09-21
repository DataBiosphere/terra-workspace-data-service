package org.databiosphere.workspacedataservice.service.model;

import java.util.Set;

public record RelationCollection(Set<Relation> relations, Set<Relation> relationArrays) {}
