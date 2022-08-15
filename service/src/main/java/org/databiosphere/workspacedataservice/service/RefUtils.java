package org.databiosphere.workspacedataservice.service;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
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
    if (obj != null){
      String sVal = obj.toString();
      int index = sVal.indexOf("/");
      if (index >= 0){
        return sVal.substring(REFERENCE_IDENTIFIER.length(), index);
      }
    }
    throw new IllegalArgumentException("Expected " + REFERENCE_IDENTIFIER + "<entityType>/<entityName>");
  }

  public static String getRefValue(Object obj) {
    if (obj != null){
      String sVal = obj.toString();
      int index = sVal.indexOf("/");
      if (index >= 0){
        return sVal.substring(index+1);
      }
    }
    throw new IllegalArgumentException("Expected " + REFERENCE_IDENTIFIER + "<entityType>/<entityName>");
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
