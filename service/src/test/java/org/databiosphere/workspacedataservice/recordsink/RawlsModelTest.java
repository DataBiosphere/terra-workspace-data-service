package org.databiosphere.workspacedataservice.recordsink;

import static org.assertj.core.api.Assertions.assertThat;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.databiosphere.workspacedataservice.common.ControlPlaneTestBase;
import org.databiosphere.workspacedataservice.recordsink.RawlsModel.AddListMember;
import org.databiosphere.workspacedataservice.recordsink.RawlsModel.AddUpdateAttribute;
import org.databiosphere.workspacedataservice.recordsink.RawlsModel.AttributeValue;
import org.databiosphere.workspacedataservice.recordsink.RawlsModel.CreateAttributeEntityReferenceList;
import org.databiosphere.workspacedataservice.recordsink.RawlsModel.CreateAttributeValueList;
import org.databiosphere.workspacedataservice.recordsink.RawlsModel.EntityReference;
import org.databiosphere.workspacedataservice.recordsink.RawlsModel.RemoveAttribute;
import org.databiosphere.workspacedataservice.recordsink.RawlsModel.RemoveListMember;
import org.databiosphere.workspacedataservice.shared.model.RecordType;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

@SpringBootTest
class RawlsModelTest extends ControlPlaneTestBase {
  @Autowired private ObjectMapper mapper;

  @Test
  void addUpdateAttribute() {
    String actualJson = jsonify(new AddUpdateAttribute("name", AttributeValue.of("value")));
    assertJsonEquals(
        "{\"op\":\"AddUpdateAttribute\",\"attributeName\":\"name\",\"addUpdateAttribute\":\"value\"}",
        actualJson);
  }

  @Test
  void addListMember() {
    String actualJson = jsonify(new AddListMember("name", AttributeValue.of("value")));

    assertJsonEquals(
        "{\"op\":\"AddListMember\",\"attributeListName\":\"name\",\"newMember\":\"value\"}",
        actualJson);
  }

  @Test
  void removeAttribute() {
    String actualJson = jsonify(new RemoveAttribute("name"));

    assertJsonEquals("{\"op\":\"RemoveAttribute\",\"attributeName\":\"name\"}", actualJson);
  }

  @Test
  void removeListMember() {
    String actualJson = jsonify(new RemoveListMember("name", AttributeValue.of("value")));

    assertJsonEquals(
        "{\"op\":\"RemoveListMember\",\"attributeListName\":\"name\",\"removeMember\":\"value\"}",
        actualJson);
  }

  @Test
  void createAttributeValueList() {
    String actualJson = jsonify(new CreateAttributeValueList("name"));

    assertJsonEquals(
        "{\"op\":\"CreateAttributeValueList\",\"attributeName\":\"name\"}", actualJson);
  }

  @Test
  void createAttributeEntityReferenceList() {
    String actualJson = jsonify(new CreateAttributeEntityReferenceList("name"));

    assertJsonEquals(
        "{\"op\":\"CreateAttributeEntityReferenceList\",\"attributeListName\":\"name\"}",
        actualJson);
  }

  @Test
  void entityReference() {
    String actualJson =
        jsonify(
            AttributeValue.of(new EntityReference(RecordType.valueOf("entityType"), "entityName")));

    assertJsonEquals("{\"entityName\":\"entityName\",\"entityType\":\"entityType\"}", actualJson);
  }

  private String jsonify(Object object) {
    try {
      return mapper.writeValueAsString(object);
    } catch (JsonProcessingException e) {
      throw new RuntimeException(e);
    }
  }

  private void assertJsonEquals(String expectedJson, String actualJson) {
    try {
      JsonNode treeExpected = mapper.readTree(expectedJson);
      JsonNode treeActual = mapper.readTree(actualJson);
      assertThat(treeActual).isEqualTo(treeExpected);
    } catch (JsonProcessingException e) {
      throw new RuntimeException(e);
    }
  }
}
