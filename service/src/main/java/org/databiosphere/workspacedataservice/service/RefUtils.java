package org.databiosphere.workspacedataservice.service;

import com.google.common.base.Preconditions;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import org.springframework.web.util.UriComponents;
import org.springframework.web.util.UriComponentsBuilder;

public class RefUtils {

  public static final String REFERENCE_IDENTIFIER = "terra-wds";

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
    Preconditions.checkArgument(obj instanceof String, errorMessage);
    String ref = (String) obj;

    // parse the string as a uri
    UriComponents uric = UriComponentsBuilder.fromUriString(ref).build();

    // uri scheme should be "terra-wds"
    Preconditions.checkArgument(REFERENCE_IDENTIFIER.equals(uric.getScheme()), errorMessage);

    // record type is the first segment of the uri path;
    // record id is the second segment of the uri path
    List<String> pathSegments = uric.getPathSegments();
    Preconditions.checkArgument(pathSegments.size() == 2, errorMessage);

    return pathSegments.toArray(new String[0]);
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

  public static String createReferenceString(String entityTypeName, String entityId) {
    return UriComponentsBuilder.newInstance()
        .scheme(REFERENCE_IDENTIFIER)
        .pathSegment(entityTypeName, entityId)
        .build()
        .toUriString();
  }
}
