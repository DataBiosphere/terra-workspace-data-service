package org.databiosphere.workspacedataservice.controller;


import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.commons.lang3.RandomUtils;
import org.databiosphere.workspacedataservice.service.RelationUtils;
import org.databiosphere.workspacedataservice.shared.model.RecordAttributes;
import org.databiosphere.workspacedataservice.shared.model.RecordRequest;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.http.*;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
public class RecordControllerExceptionTest {
    @Autowired
    private TestRestTemplate restTemplate;
    private static HttpHeaders headers;
    private static UUID instanceId;

    private static String versionId = "v0.2";

    private final ObjectMapper mapper = new ObjectMapper();

    @BeforeAll
    static void setUp() {
        headers = new HttpHeaders();
        headers.setContentType(MediaType.APPLICATION_JSON);
        instanceId = UUID.randomUUID();
    }

    @Test
    void missingReferencedRecordTypeShouldFail() throws JsonProcessingException {
        Map<String, Object> attrs = new HashMap<>();
        attrs.put("attr_ref", RelationUtils.createRelationString("non_existent", "recordId"));
        attrs.put("attr_ref_2", RelationUtils.createRelationString("non_existent_2", "recordId"));
        HttpEntity<String> requestEntity = new HttpEntity<>(mapper.writeValueAsString(new RecordRequest(new RecordAttributes(attrs))), headers);
        ResponseEntity<LinkedHashMap> response = restTemplate.exchange("/{instanceId}/records/{version}/{recordType}/{recordId}",
                HttpMethod.PUT, requestEntity, LinkedHashMap.class, instanceId, versionId, "samples-1", "sample_1");
        LinkedHashMap responseContent = response.getBody();
        assertThat(responseContent.get("message")).isEqualTo("Referenced table(s) [non_existent_2, non_existent] do(es) not exist");
        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.BAD_REQUEST);
    }

    @Test
    void referencingMissingRecordShouldFail() throws Exception {
        Map<String, Object> attrs = new HashMap<>();
        String referencedRecordType = "referenced-type";
        createSomeRecords(referencedRecordType, 1);
        attrs.put("attr_ref", RelationUtils.createRelationString(referencedRecordType, "missing-id"));
        HttpEntity<String> requestEntity = new HttpEntity<>(mapper.writeValueAsString(new RecordRequest(new RecordAttributes(attrs))), headers);
        ResponseEntity<LinkedHashMap> response = restTemplate.exchange("/{instanceId}/records/{version}/{recordType}/{recordId}",
                HttpMethod.PUT, requestEntity, LinkedHashMap.class, instanceId, versionId, "samples-2", "sample_1");
        LinkedHashMap responseContent = response.getBody();
        assertThat(responseContent.get("message")).isEqualTo("It looks like you're trying to reference a record that does not exist.");
        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.BAD_REQUEST);
    }


    @Test
    void retrievingMissingEntityShouldFail() throws Exception {
        createSomeRecords("samples", 1);
        HttpEntity<String> requestEntity = new HttpEntity<>(headers);
        ResponseEntity<LinkedHashMap> response = restTemplate.exchange("/{instanceId}/records/{version}/{recordType}/{recordId}",
                HttpMethod.GET, requestEntity, LinkedHashMap.class, instanceId, versionId, "samples", "sample_1");
        LinkedHashMap responseContent = response.getBody();
        assertThat(responseContent.get("message")).isEqualTo("Record not found");
        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.NOT_FOUND);
    }

    private void createSomeRecords(String recordType, int numRecords) throws Exception {
        for (int i = 0; i < numRecords; i++) {
            String recordId = "record_" + i;
            Map<String, Object> attributes = new HashMap<>();
            attributes.put("attr1", RandomStringUtils.randomAlphabetic(6));
            attributes.put("attr2", RandomUtils.nextFloat());
            attributes.put("attr3", "2022-11-01");
            attributes.put("attr4", RandomStringUtils.randomNumeric(5));
            attributes.put("attr5", RandomUtils.nextLong());
            attributes.put("attr-dt", "2022-03-01T12:00:03");
            attributes.put("attr-json", "{\"foo\":\"bar\"}");
            attributes.put("attr-boolean", true);
            ResponseEntity<String> response = restTemplate.exchange("/{instanceId}/records/{version}/{recordType}/{recordId}", HttpMethod.PUT,
                    new HttpEntity<>(mapper.writeValueAsString(new RecordRequest(new RecordAttributes(attributes))), headers), String.class, instanceId, versionId,
                    recordType, recordId);
            assertThat(response.getStatusCode()).isEqualTo(HttpStatus.CREATED);
        }
    }
}
