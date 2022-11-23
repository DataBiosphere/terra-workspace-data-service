package org.databiosphere.workspacedataservice.service;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.google.common.base.Preconditions;
import org.databiosphere.workspacedataservice.service.model.Relation;
import org.databiosphere.workspacedataservice.service.model.RelationCollection;
import org.databiosphere.workspacedataservice.service.model.exception.InvalidRelationException;
import org.databiosphere.workspacedataservice.shared.model.Record;
import org.databiosphere.workspacedataservice.shared.model.RecordType;
import org.springframework.web.util.UriComponents;
import org.springframework.web.util.UriComponentsBuilder;

public class RelationUtils {

	public static final String RELATION_IDENTIFIER = "terra-wds";

	/**
	 * Determines if any attributes reference another table
	 *
	 * @param records
	 *            - all records whose references to check
	 * @return Set of Relation for all referencing attributes
	 */
	public static RelationCollection findRelations(List<Record> records) {
		Set<Relation> relations = new HashSet<>();
		Set<Relation> relationArrays = new HashSet<>();
		for (Record record : records) {
			for (Map.Entry<String, Object> entry : record.attributeSet()) {
				if (isRelationValue(entry.getValue())) {
					relations.add(new Relation(entry.getKey(), getTypeValue(entry.getValue())));
					//TODO distinguish between tsv & json source?
				} else if (entry.getValue() instanceof List<?> listVal && !listVal.isEmpty() && listVal.stream().allMatch(RelationUtils::isRelationValue)){
					relationArrays.add(new Relation(entry.getKey(), getTypeValueForArray(listVal)));
				}
			}
		}
		return new RelationCollection(relations, relationArrays);
	}

	public static RecordType getTypeValue(Object obj) {
		return RecordType.valueOf(splitRelationIdentifier(obj)[0]);
	}

	public static RecordType getTypeValueForArray(List<?> listVal) {
		if (listVal.stream().map(RelationUtils::getTypeValue).distinct().count() > 1){
			throw new InvalidRelationException("All relations in an array must relate to the same table");
		}
		return getTypeValue(listVal.get(0));
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
		return splitRelationIdentifier(obj)[1];
	}

	/**
	 * Determines whether attribute value matches this expectation
	 *
	 * @param obj
	 *            - attribute value to check
	 * @return true if attribute begins with the REFERENCE_IDENTIFIER
	 */
	public static boolean isRelationValue(Object obj) {
		return obj != null && obj.toString().startsWith(RELATION_IDENTIFIER);
	}

	public static String createRelationString(RecordType targetRecordType, String recordId) {
		return UriComponentsBuilder.newInstance().scheme(RELATION_IDENTIFIER)
				.pathSegment(targetRecordType.getName(), recordId).build().toUriString();
	}
}
