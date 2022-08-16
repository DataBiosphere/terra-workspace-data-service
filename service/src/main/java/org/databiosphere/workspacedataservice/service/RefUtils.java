package org.databiosphere.workspacedataservice.service;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.google.common.base.Preconditions;
import org.databiosphere.workspacedataservice.service.model.EntityReference;
import org.databiosphere.workspacedataservice.shared.model.Entity;
import org.databiosphere.workspacedataservice.shared.model.EntityType;

public class RefUtils {

  public static final String REFERENCE_IDENTIFIER = "terra-wds:";

  /**
   * Determines if any attributes reference another table
   *
   * @param entities - all entities whose references to check
   * @return Set of EntityReference for all referencing attributes
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
    return splitReferenceString(obj)[0];
  }

  private static String[] splitReferenceString(Object obj) {
    String errorMessage = "Expected " + REFERENCE_IDENTIFIER + "<entityType>/<entityName>";
    Preconditions.checkNotNull(obj, errorMessage);
    String[] parts = obj.toString().substring(REFERENCE_IDENTIFIER.length()).split("/");
    Preconditions.checkArgument(parts.length == 2, errorMessage);
    return parts;
  }

  public static String getRefValue(Object obj) {
    return splitReferenceString(obj)[1];
  }

  /**
   * Determines whether attribute value matches this expectation
   *
   * @param obj - attribute value to check
   * @return true if attribute begins with the REFERENCE_IDENTIFIER
   */
  public static boolean isReferenceValue(Object obj) {
    return obj != null && obj.toString().startsWith(REFERENCE_IDENTIFIER);
  }

  public static String createReferenceString(String entityTypeName, String entityId){
    return REFERENCE_IDENTIFIER + entityTypeName + "/" + entityId;
  }
}
