package org.databiosphere.workspacedataservice.controller;

import com.univocity.parsers.tsv.TsvParser;
import com.univocity.parsers.tsv.TsvParserSettings;
import org.databiosphere.workspacedataservice.shared.model.BatchResponse;
import org.databiosphere.workspacedataservice.shared.model.RecordType;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.core.io.Resource;
import org.springframework.http.*;

import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.UUID;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
public class TsvDownloadTest {

    @Autowired
    private TestRestTemplate restTemplate;

    @Autowired
    private RecordController recordController;

    @Test
    void batchWriteFollowedByTsvDownload() throws IOException {
        RecordType recordType = RecordType.valueOf("tsv-test");
        String version = "v0.2";
        UUID instanceId = UUID.randomUUID();
        System.out.println(instanceId);
        recordController.createInstance(instanceId, version);
        InputStream is = FullStackRecordControllerTest.class.getResourceAsStream("/batch_write_tsv_data.json");
        ResponseEntity<BatchResponse> response = recordController.streamingWrite(instanceId, version, recordType, is);
        is.close();
        assertThat(response.getStatusCodeValue()).isEqualTo(200);
        HttpHeaders headers = new HttpHeaders();
        headers.setContentType(MediaType.APPLICATION_JSON);
        ResponseEntity<Resource> stream = restTemplate.exchange("/{instanceId}/records/{version}/{recordType}",
                HttpMethod.GET, new HttpEntity<>(headers), Resource.class, instanceId, version, recordType);
        InputStream inputStream = stream.getBody().getInputStream();
        TsvParserSettings tsvParserSettings = new TsvParserSettings();
        tsvParserSettings.getFormat().setLineSeparator("\n");
        TsvParser tsvParser = new TsvParser(tsvParserSettings);
        List<String[]> result = tsvParser.parseAll(inputStream);
        assertThat(result.size()).isEqualTo(3);
        assertThat(result.get(0)).isEqualTo(new String[]{"sys_name", "createdAt", "description", "location", "jsonObj", "jsonArray"});
        assertThat(result.get(1)).isEqualTo(new String[]{"1", "2021-10-11", "Embedded\tTab", "Portland, OR", "{\"age\": 22, \"foo\": \"bar\"}", null});
        assertThat(result.get(2)).isEqualTo(new String[]{"2", null, "\n,Weird\n String", "Cambridge, MA", null, "[1, 3, 9, \"puppies\"]"});
    }
}
