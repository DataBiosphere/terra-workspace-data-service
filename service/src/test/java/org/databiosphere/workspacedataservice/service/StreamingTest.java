package org.databiosphere.workspacedataservice.service;

import org.databiosphere.workspacedataservice.dao.RecordDao;
import org.databiosphere.workspacedataservice.shared.model.Record;
import org.databiosphere.workspacedataservice.shared.model.RecordType;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

@SpringBootTest
public class StreamingTest {

    @Autowired
    private RecordDao recordDao;

    @Test
    void testFoo() throws IOException {
        StreamingWriteHandler handler = new StreamingWriteHandler(StreamingTest.class.getResourceAsStream("/batch_write.json"));
        List<Record> records = handler.readRecords(1).getRecords();
        assertThat(records).as("Should only return 1 record").hasSize(1);
    }

    @Test
    void testReadWholeStream() throws IOException {
        StreamingWriteHandler handler = new StreamingWriteHandler(StreamingTest.class.getResourceAsStream("/batch_write.json"));
        boolean haveRecords = true;
        int iterations = 0;
        while(haveRecords){
            List<Record> records = handler.readRecords(2).getRecords();
            iterations++;
            haveRecords = !records.isEmpty();
        }
        assertThat(iterations).isEqualTo(3);
    }

    @Test
    void serviceTest() {
        InputStream stream = StreamingTest.class.getResourceAsStream("/batch_write.json");
        new StreamingWriteService(recordDao).consumeStream(stream, 2,
                UUID.fromString("0d94b421-6e19-43b9-99c4-842dba662ca8"), RecordType.valueOf("foobo"));
    }
}
