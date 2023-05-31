package org.databiosphere.workspacedataservice.service;

import org.databiosphere.workspacedataservice.process.LocalProcessLauncher;
import org.databiosphere.workspacedataservice.storage.LocalFileStorage;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.test.context.TestPropertySource;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.InputStream;
import java.util.List;
import java.util.Scanner;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.*;

@SpringBootTest(properties = "spring.cache.type=NONE")
@TestPropertySource("classpath:test.properties")
class BackupServiceTest {
    @Autowired
    private BackupService mockBackupService;

    @MockBean
    private LocalFileStorage blobServiceClient;
    @Test
    void testBackupAzureWDS() throws Exception {
        String text = "test-data";
        InputStream inputStream = new ByteArrayInputStream(text.getBytes());

        LocalProcessLauncher process = mock(LocalProcessLauncher.class);
        when(process.getInputStream()).thenReturn(inputStream);

        blobServiceClient.streamOutputToBlobStorage(inputStream, "backup");

        File myObj = new File("backup.txt");
        Scanner myReader = new Scanner(myObj);
        while (myReader.hasNextLine()) {
            String data = myReader.nextLine();
            System.out.println(data);
            assertThat(data) .isEqualTo(text);
        }
        myReader.close();
    }

    @Test
    void CheckCommandLine() {
        List<String> commandList = mockBackupService.GenerateCommandList();
        String command = String.join(" ", commandList);
        assert(command).equals("/usr/bin/pg_dump -h localhost -p 5432 -U postgres -d wds -v -w");
    }
}
