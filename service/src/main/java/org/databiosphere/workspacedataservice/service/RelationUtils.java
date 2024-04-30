package org.databiosphere.workspacedataservice.service;

import com.google.common.base.Preconditions;
import java.util.Arrays;
import java.util.List;
import org.databiosphere.workspacedataservice.service.model.exception.InvalidRelationException;
import org.databiosphere.workspacedataservice.shared.model.RecordType;
import org.databiosphere.workspacedataservice.shared.model.attributes.RelationAttribute;
import org.springframework.web.util.UriComponents;
import org.springframework.web.util.UriComponentsBuilder;

public class RelationUtils {

  public static final String RELATION_IDENTIFIER = "terra-wds";

  public static RecordType getTypeValue(Object obj) {
    return RecordType.valueOf(splitRelationIdentifier(obj.toString())[0]);
  }

  public static RecordType getTypeValueForList(List<?> listVal) {
    if (listVal.stream().map(RelationUtils::getTypeValue).distinct().count() > 1) {
      throw new InvalidRelationException("All relations in an array must relate to the same table");
    }
    return getTypeValue(listVal.get(0));
  }

  public static RecordType getTypeValueForArray(String[] arr) {
    return getTypeValueForList(Arrays.asList(arr));
  }

  private static String[] splitRelationIdentifier(Object obj) {
    String errorMessage = "Expected " + RELATION_IDENTIFIER + "<recordType>/<recordId>";
    Preconditions.checkNotNull(obj, errorMessage);
    Preconditions.checkArgument(obj instanceof String, errorMessage);
    String ref = (String) obj;

    // parse the string as a uri
    UriComponents uric = UriComponentsBuilder.fromUriString(ref).build();

    // uri scheme should be "terra-wds"
    Preconditions.checkArgument(RELATION_IDENTIFIER.equals(uric.getScheme()), errorMessage);

    // record type is the first segment of the uri path;
    // record id is the second segment of the uri path
    List<String> pathSegments = uric.getPathSegments();
    Preconditions.checkArgument(pathSegments.size() == 2, errorMessage);

    return pathSegments.toArray(new String[0]);
  }

  public static String getRelationValue(Object obj) {
    return splitRelationIdentifier(obj.toString())[1];
  }

  /**
   * Determines whether attribute value matches this expectation
   *
   * @param obj - attribute value to check
   * @return true if attribute begins with the REFERENCE_IDENTIFIER
   */
  public static boolean isRelationValue(Object obj) {
    return obj instanceof RelationAttribute || obj.toString().startsWith(RELATION_IDENTIFIER);
  }

  public static String createRelationString(RecordType targetRecordType, String recordId) {
    return UriComponentsBuilder.newInstance()
        .scheme(RELATION_IDENTIFIER)
        .pathSegment(targetRecordType.getName(), recordId)
        .build()
        .toUriString();
  }
}
