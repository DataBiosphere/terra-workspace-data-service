package org.databiosphere.workspacedataservice.service;

import static org.databiosphere.workspacedataservice.service.model.EntityReference.ENTITY_NAME_KEY;
import static org.databiosphere.workspacedataservice.service.model.EntityReference.ENTITY_TYPE_KEY;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.databiosphere.workspacedataservice.service.model.EntityReference;
import org.databiosphere.workspacedataservice.shared.model.Entity;
import org.databiosphere.workspacedataservice.shared.model.EntityType;

public class RefUtils {

  /**
   * Determines if any attributes reference another table
   *
   * @param entities - all entities whose references to check
   * @return Set of SingleTenantEntityReference for all referencing attributes
   */
  public static Set<EntityReference> findEntityReferences(List<Entity> entities) {
    Set<EntityReference> result = new HashSet<>();
    for (Entity entity : entities) {
      Map<String, Object> attributes = entity.getAttributes().getAttributes();
      for (String attr : attributes.keySet()) {
        if (isReferenceValue(attributes.get(attr))) {
          result.add(new EntityReference(attr, new EntityType(getTypeValue(attributes.get(attr)))));
        }
      }
    }
    return result;
  }

  public static String getTypeValue(Object obj) {
    if (obj instanceof Map) {
      Map map = (Map) obj;
      return (String) map.get(ENTITY_TYPE_KEY);
    }
    throw new IllegalArgumentException("Expected {\"entityType\":<type>, \"entityName\":<name>}");
  }

  public static String getRefValue(Object obj) {
    if (obj instanceof Map) {
      Map map = (Map) obj;
      return (String) map.get(ENTITY_NAME_KEY);
    }
    throw new IllegalArgumentException("Expected {\"entityType\":<type>, \"entityName\":<name>}");
  }

  /**
   * Determines whether attribute value matches this expectation
   *
   * @param obj - attribute value to check
   * @return true if attribute in form of a map with keys "entityType" and "entityName"
   */
  public static boolean isReferenceValue(Object obj) {
    if (obj instanceof Map) {
      Map map = (Map) obj;
      return map.size() == 2 && map.keySet().containsAll(Set.of(ENTITY_TYPE_KEY, ENTITY_NAME_KEY));
    }
    return false;
  }
}
