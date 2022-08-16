package org.databiosphere.workspacedataservice.service;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.google.common.base.Preconditions;
import org.databiosphere.workspacedataservice.service.model.Relation;
import org.databiosphere.workspacedataservice.shared.model.Record;
import org.databiosphere.workspacedataservice.shared.model.RecordType;

public class RelationUtils {

  public static final String REFERENCE_IDENTIFIER = "terra-wds:";

  /**
   * Determines if any attributes reference another table
   *
   * @param entities - all entities whose references to check
   * @return Set of EntityReference for all referencing attributes
   */
  public static Set<Relation> findRelations(List<Record> entities) {
    Set<Relation> result = new HashSet<>();
    for (Record record : entities) {
      Map<String, Object> attributes = record.getAttributes().getAttributes();
      for (String attr : attributes.keySet()) {
        if (isRelationValue(attributes.get(attr))) {
          result.add(new Relation(attr, new RecordType(getTypeValue(attributes.get(attr)))));
        }
      }
    }
    return result;
  }

  public static String getTypeValue(Object obj) {
    return splitRelationIdentifier(obj)[0];
  }

  private static String[] splitRelationIdentifier(Object obj) {
    String errorMessage = "Expected " + REFERENCE_IDENTIFIER + "<recordType>/<recordName>";
    Preconditions.checkNotNull(obj, errorMessage);
    String[] parts = obj.toString().substring(REFERENCE_IDENTIFIER.length()).split("/");
    Preconditions.checkArgument(parts.length == 2, errorMessage);
    return parts;
  }

  public static String getRelationValue(Object obj) {
    return splitRelationIdentifier(obj)[1];
  }

  /**
   * Determines whether attribute value matches this expectation
   *
   * @param obj - attribute value to check
   * @return true if attribute begins with the REFERENCE_IDENTIFIER
   */
  public static boolean isRelationValue(Object obj) {
    return obj != null && obj.toString().startsWith(REFERENCE_IDENTIFIER);
  }

  public static String createRelationString(String entityTypeName, String entityId){
    return REFERENCE_IDENTIFIER + entityTypeName + "/" + entityId;
  }
}
