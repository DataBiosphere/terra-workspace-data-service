package org.databiosphere.workspacedataservice.recordsink;

import static java.util.Objects.requireNonNull;
import static org.databiosphere.workspacedataservice.recordsink.RawlsModel.Op.ADD_LIST_MEMBER;
import static org.databiosphere.workspacedataservice.recordsink.RawlsModel.Op.ADD_UPDATE_ATTRIBUTE;
import static org.databiosphere.workspacedataservice.recordsink.RawlsModel.Op.CREATE_ATTRIBUTE_ENTITY_REFERENCE_LIST;
import static org.databiosphere.workspacedataservice.recordsink.RawlsModel.Op.CREATE_ATTRIBUTE_VALUE_LIST;
import static org.databiosphere.workspacedataservice.recordsink.RawlsModel.Op.REMOVE_ATTRIBUTE;
import static org.databiosphere.workspacedataservice.recordsink.RawlsModel.Op.REMOVE_LIST_MEMBER;
import static org.databiosphere.workspacedataservice.service.RelationUtils.getRelationValue;
import static org.databiosphere.workspacedataservice.service.RelationUtils.getTypeValue;

import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonSubTypes.Type;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.fasterxml.jackson.annotation.JsonTypeInfo.Id;
import com.fasterxml.jackson.annotation.JsonValue;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.deser.std.StdDeserializer;
import com.fasterxml.jackson.databind.util.TokenBuffer;
import java.io.IOException;
import java.util.List;
import org.databiosphere.workspacedataservice.shared.model.RecordType;
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
      return String.valueOf(value);
    }
  }

  /** Top level class for describing a Rawls batch upsert for a particular Entity. */
  public record Entity(
      String name, String entityType, List<? extends AttributeOperation> operations) {}

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
  }

  // Concrete subclasses of AttributeOperation
  public record AddUpdateAttribute(
      String attributeName,
      @JsonDeserialize(using = RawlsValueDeserializer.class) Object addUpdateAttribute)
      implements AttributeOperation {

    @Override
    public Op op() {
      return ADD_UPDATE_ATTRIBUTE;
    }
  }

  public record AddListMember(
      String attributeListName,
      @JsonDeserialize(using = RawlsValueDeserializer.class) Object newMember)
      implements AttributeOperation {

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

  public record RemoveListMember(
      String attributeListName,
      @JsonDeserialize(using = RawlsValueDeserializer.class) Object removeMember)
      implements AttributeOperation {
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

  public record CreateAttributeEntityReferenceList(String attributeName)
      implements AttributeOperation {
    @Override
    public Op op() {
      return CREATE_ATTRIBUTE_ENTITY_REFERENCE_LIST;
    }
  }

  /**
   * A reference to a record in the workspace data service, used to serialize a reference to Rawls
   * upsert JSON.
   */
  public record RecordReference(RecordType entityType, String entityName) {

    /** Parse a wds reference string into a {@link RecordReference}. */
    public static RecordReference fromReferenceString(String referenceString) {
      return new RecordReference(getTypeValue(referenceString), getRelationValue(referenceString));
    }
  }

  static class RawlsValueDeserializer extends StdDeserializer<Object> {
    @Nullable private transient JsonDeserializer<Object> defaultDeserializer;

    public RawlsValueDeserializer() {
      super(Object.class);
    }

    @Override
    public Object deserialize(JsonParser parser, DeserializationContext context)
        throws IOException {
      // Temporary tree model to inspect the JSON
      JsonNode node = parser.getCodec().readTree(parser);
      if (isReferenceObject(node)) {
        return toRecordReference(node);
      }
      // Delegate to default deserializer
      return defaultDeserialization(parser, context, node);
    }

    private boolean isReferenceObject(JsonNode node) {
      return node.isObject() && node.has("entityType") && node.has("entityName");
    }

    private RecordReference toRecordReference(JsonNode node) {
      return new RecordReference(
          RecordType.valueOf(node.get("entityType").asText()), node.get("entityName").asText());
    }

    private Object defaultDeserialization(
        JsonParser parser, DeserializationContext context, JsonNode node) throws IOException {
      if (defaultDeserializer == null) {
        defaultDeserializer = getDefaultDeserializer(context);
      }
      try (TokenBuffer tokenBuffer =
          new TokenBuffer(parser.getCodec(), /* hasNativeIds= */ false)) {
        tokenBuffer.writeTree(node);
        JsonParser parserForDefaultDeserialization = tokenBuffer.asParser();
        parserForDefaultDeserialization.nextToken(); // Prepare the parser to read the first token

        return defaultDeserializer.deserialize(parserForDefaultDeserialization, context);
      }
    }

    private static JsonDeserializer<Object> getDefaultDeserializer(DeserializationContext context)
        throws JsonMappingException {
      return requireNonNull(context.findRootValueDeserializer(context.constructType(Object.class)));
    }
  }
}
