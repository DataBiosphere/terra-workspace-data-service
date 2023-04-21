package org.databiosphere.workspacedataservice.service;

import com.azure.storage.blob.BlobServiceClient;
import com.azure.storage.blob.BlobServiceClientBuilder;
import com.azure.storage.blob.specialized.BlockBlobClient;
import org.databiosphere.workspacedataservice.process.LocalProcessLauncher;
import org.junit.jupiter.api.BeforeEach;
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

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

@TestPropertySource("classpath:test.properties")
@ExtendWith(SpringExtension.class)
@SpringBootTest
public class BackupServiceTest {

    UUID workspaceId = UUID.randomUUID();

    @Autowired
    private BackupService backupService;

    @MockBean
    private LocalProcessLauncher localProcessLauncher;

    // Probably need to revisit, build some type of wrapper
    // this class in final, so it can't be mocked out via Mockito :(
    @MockBean
    private BlobServiceClientBuilder blobServiceClientBuilder;

    @MockBean
    private BlobServiceClient blobServiceClient;

    @Test
    public void testBackupAzureWDS() {
        // this is failing
        InputStream inputStream = new ByteArrayInputStream("test-data".getBytes());
        when(localProcessLauncher.launchProcess(any(), any(), any())).thenReturn(inputStream);

        BlobServiceClientBuilder builder = mock(BlobServiceClientBuilder.class);
        when(builder.connectionString(any())).thenReturn(builder);
        when(builder.buildClient()).thenReturn(blobServiceClient);
        when(blobServiceClientBuilder.buildClient()).thenReturn(blobServiceClient);

        backupService.backupAzureWDS(workspaceId);

        verify(localProcessLauncher).launchProcess(any(), any(), any());
    }

    @Test
    public void testConstructBlockBlobClient() {
        // this is failing as well
        String blobName = "test-blob";
        BlobServiceClientBuilder builder = mock(BlobServiceClientBuilder.class);
        when(builder.connectionString(any())).thenReturn(builder);
        when(builder.buildClient()).thenReturn(blobServiceClient);
        when(blobServiceClientBuilder.buildClient()).thenReturn(blobServiceClient);

        BlockBlobClient blockBlobClient = backupService.constructBlockBlobClient(blobName);

        assertNotNull(blockBlobClient);
    }
}
