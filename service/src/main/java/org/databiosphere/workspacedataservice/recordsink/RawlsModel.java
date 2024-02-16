package org.databiosphere.workspacedataservice.recordsink;

import static org.databiosphere.workspacedataservice.recordsink.RawlsModel.Op.ADD_LIST_MEMBER;
import static org.databiosphere.workspacedataservice.recordsink.RawlsModel.Op.ADD_UPDATE_ATTRIBUTE;
import static org.databiosphere.workspacedataservice.recordsink.RawlsModel.Op.CREATE_ATTRIBUTE_ENTITY_REFERENCE_LIST;
import static org.databiosphere.workspacedataservice.recordsink.RawlsModel.Op.CREATE_ATTRIBUTE_VALUE_LIST;
import static org.databiosphere.workspacedataservice.recordsink.RawlsModel.Op.REMOVE_ATTRIBUTE;
import static org.databiosphere.workspacedataservice.recordsink.RawlsModel.Op.REMOVE_LIST_MEMBER;

import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonSubTypes.Type;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.fasterxml.jackson.annotation.JsonTypeInfo.Id;
import com.fasterxml.jackson.annotation.JsonValue;
import java.util.List;

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
  public record AddUpdateAttribute(String attributeName, Object addUpdateAttribute)
      implements AttributeOperation {

    @Override
    public Op op() {
      return ADD_UPDATE_ATTRIBUTE;
    }
  }

  public record AddListMember(String attributeListName, Object newMember)
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

  public record RemoveListMember(String attributeListName, Object removeMember)
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
}
