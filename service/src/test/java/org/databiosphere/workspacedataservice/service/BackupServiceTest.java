package org.databiosphere.workspacedataservice.service;

import org.databiosphere.workspacedataservice.process.LocalProcessLauncher;
import org.databiosphere.workspacedataservice.storage.LocalFileStorage;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.context.junit.jupiter.SpringExtension;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.util.UUID;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

@TestPropertySource("classpath:test.properties")
public class BackupServiceTest {
    @MockBean
    private LocalFileStorage blobServiceClient = new LocalFileStorage();

    @Test
    public void testBackupAzureWDS() throws Exception {
       InputStream inputStream = new ByteArrayInputStream("test-data".getBytes());

        LocalProcessLauncher process = mock(LocalProcessLauncher.class);
        when(process.getInputStream()).thenReturn(inputStream);

        blobServiceClient.streamOutputToBlobStorage(inputStream, "backup");
    }
}
