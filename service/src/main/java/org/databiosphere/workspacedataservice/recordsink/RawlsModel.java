package org.databiosphere.workspacedataservice.recordsink;

import static org.databiosphere.workspacedataservice.recordsink.RawlsModel.Op.ADD_LIST_MEMBER;
import static org.databiosphere.workspacedataservice.recordsink.RawlsModel.Op.ADD_UPDATE_ATTRIBUTE;
import static org.databiosphere.workspacedataservice.recordsink.RawlsModel.Op.CREATE_ATTRIBUTE_ENTITY_REFERENCE_LIST;
import static org.databiosphere.workspacedataservice.recordsink.RawlsModel.Op.CREATE_ATTRIBUTE_VALUE_LIST;
import static org.databiosphere.workspacedataservice.recordsink.RawlsModel.Op.REMOVE_ATTRIBUTE;
import static org.databiosphere.workspacedataservice.recordsink.RawlsModel.Op.REMOVE_LIST_MEMBER;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonSubTypes.Type;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.fasterxml.jackson.annotation.JsonTypeInfo.Id;
import com.fasterxml.jackson.annotation.JsonValue;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.annotations.VisibleForTesting;
import java.io.IOException;
import java.util.List;
import org.databiosphere.workspacedataservice.shared.model.RecordType;
import org.databiosphere.workspacedataservice.shared.model.attributes.RelationAttribute;
import org.springframework.lang.Nullable;

/**
 * Model class to implement a JSON payload compatible with Rawls <a
 * href="https://rawls.dsde-prod.broadinstitute.org/#/entities/batch_upsert_entities">batchUpsertEntities</a>
 * endpoint.
 *
 * <p>Based on: <a
 * href="https://github.com/broadinstitute/rawls/blob/develop/model/src/main/scala/org/broadinstitute/dsde/rawls/model/AttributeUpdateOperations.scala">
 * AttributeUpdateOperations.scala</a>
 */
public class RawlsModel {
  /**
   * Enum to represent the operation type, with a value to be used in JSON serialization. Each
   * {@link Op} corresponds with a particular subclass of {@link AttributeOperation} and is used to
   * specify the JSON value of its "op" attribute when an instance of that subclass is serialized to
   * JSON.
   */
  public enum Op {
    ADD_UPDATE_ATTRIBUTE("AddUpdateAttribute"),
    ADD_LIST_MEMBER("AddListMember"),
    REMOVE_ATTRIBUTE("RemoveAttribute"),
    REMOVE_LIST_MEMBER("RemoveListMember"),
    CREATE_ATTRIBUTE_VALUE_LIST("CreateAttributeValueList"),
    CREATE_ATTRIBUTE_ENTITY_REFERENCE_LIST("CreateAttributeEntityReferenceList");
    private final String value;

    Op(String value) {
      this.value = value;
    }

    @JsonValue
    public String value() {
      return value;
    }

    @Override
    public String toString() {
      return value;
    }
  }

  /** Top level class for describing a Rawls batch upsert for a particular Entity. */
  public record Entity(String name, String entityType, List<AttributeOperation> operations) {}

  /** Deserialize into concrete subclasses based on the value of the "op" attribute in the JSON. */
  @JsonTypeInfo(use = Id.NAME, property = "op")
  @JsonSubTypes({
    @Type(value = AddUpdateAttribute.class, name = "AddUpdateAttribute"),
    @Type(value = AddListMember.class, name = "AddListMember"),
    @Type(value = RemoveAttribute.class, name = "RemoveAttribute"),
    @Type(value = RemoveListMember.class, name = "RemoveListMember"),
    @Type(value = CreateAttributeValueList.class, name = "CreateAttributeValueList"),
    @Type(
        value = CreateAttributeEntityReferenceList.class,
        name = "CreateAttributeEntityReferenceList")
  })
  @JsonPropertyOrder("op") // make op first field to be rendered for readability
  public sealed interface AttributeOperation
      permits AddUpdateAttribute,
          AddListMember,
          RemoveAttribute,
          RemoveListMember,
          CreateAttributeValueList,
          CreateAttributeEntityReferenceList {
    Op op();

    @VisibleForTesting
    String attributeName();
  }

  @JsonDeserialize(using = AttributeValueDeserializer.class)
  @JsonInclude(Include.NON_NULL)
  public record AttributeValue(Object value) {
    public static AttributeValue of(Object value) {
      // Here, you could add additional type checks or transformations as needed
      return new AttributeValue(value);
    }

    /** expose {@link #value() as the JSON value} */
    @JsonValue
    public Object getValue() {
      return value();
    }
  }

  // Concrete subclasses of AttributeOperation
  public record AddUpdateAttribute(String attributeName, AttributeValue addUpdateAttribute)
      implements AttributeOperation {
    @Override
    public Op op() {
      return ADD_UPDATE_ATTRIBUTE;
    }
  }

  public record AddListMember(String attributeListName, AttributeValue newMember)
      implements AttributeOperation {

    @Override
    @JsonIgnore // prefer attributeListName for serialization
    public String attributeName() {
      return attributeListName;
    }

    @Override
    public Op op() {
      return ADD_LIST_MEMBER;
    }
  }

  public record RemoveAttribute(String attributeName) implements AttributeOperation {
    @Override
    public Op op() {
      return REMOVE_ATTRIBUTE;
    }
  }

  public record RemoveListMember(String attributeListName, AttributeValue removeMember)
      implements AttributeOperation {

    @Override
    @JsonIgnore // prefer attributeListName for serialization
    public String attributeName() {
      return attributeListName;
    }

    @Override
    public Op op() {
      return REMOVE_LIST_MEMBER;
    }
  }

  public record CreateAttributeValueList(String attributeName) implements AttributeOperation {
    @Override
    public Op op() {
      return CREATE_ATTRIBUTE_VALUE_LIST;
    }
  }

  public record CreateAttributeEntityReferenceList(String attributeListName)
      implements AttributeOperation {

    @Override
    @JsonIgnore // prefer attributeListName for serialization
    public String attributeName() {
      return attributeListName;
    }

    @Override
    public Op op() {
      return CREATE_ATTRIBUTE_ENTITY_REFERENCE_LIST;
    }
  }

  /**
   * A reference to a record in the workspace data service, used to serialize a reference to Rawls
   * upsert JSON.
   */
  public record EntityReference(RecordType entityType, String entityName) {
    public static EntityReference fromRelationAttribute(RelationAttribute relationAttribute) {
      return new EntityReference(
          relationAttribute.getTargetType(), relationAttribute.getTargetId());
    }
  }

  /**
   * This deserializer allows expressive testing of serialized JSON, no parts of the current
   * production runtime require the Rawls JSON to be deserialized.
   */
  @VisibleForTesting
  static class AttributeValueDeserializer extends JsonDeserializer<AttributeValue> {

    @Override
    @Nullable
    public AttributeValue deserialize(JsonParser parser, DeserializationContext context)
        throws IOException {
      JsonToken currentToken = parser.currentToken();

      return switch (currentToken) {
        case VALUE_STRING -> AttributeValue.of(parser.getValueAsString());
        case VALUE_NUMBER_INT -> AttributeValue.of(parser.getIntValue());
        case VALUE_NUMBER_FLOAT -> AttributeValue.of(parser.getDoubleValue());
        case VALUE_TRUE, VALUE_FALSE -> AttributeValue.of(parser.getBooleanValue());
        case VALUE_NULL -> null;
        case START_OBJECT -> {
          ObjectNode node = parser.readValueAsTree();
          if (node.has("entityName") && node.has("entityType")) {
            ObjectMapper mapper = (ObjectMapper) parser.getCodec();
            EntityReference entityReference = mapper.treeToValue(node, EntityReference.class);
            yield AttributeValue.of(entityReference);
          } else {
            yield AttributeValue.of(node.toString());
          }
        }
        default ->
            throw new RawlsDeserializationException("Unexpected JSON token: " + currentToken) {};
      };
    }
  }

  /** Exception thrown when deserializing Rawls JSON. */
  @VisibleForTesting
  public static class RawlsDeserializationException extends RuntimeException {
    public RawlsDeserializationException(String message) {
      super(message);
    }
  }
}
