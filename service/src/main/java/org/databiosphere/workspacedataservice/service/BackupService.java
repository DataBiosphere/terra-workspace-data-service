package org.databiosphere.workspacedataservice.service;

import org.databiosphere.workspacedataservice.process.LocalProcessLauncher;
import com.azure.storage.blob.BlobContainerClient;
import com.azure.storage.blob.BlobServiceClient;
import com.azure.storage.blob.BlobServiceClientBuilder;
import com.azure.storage.blob.specialized.BlockBlobClient;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.util.List;
import java.util.HashMap;
import java.util.Map;
import java.nio.file.Path;
import java.nio.file.Paths;

@Component
public class BackupService {

    // TODO: Replace with application.properties value perhaps? Or a value from k8s?
    @Value("${AZURE_STORAGE_CONNECTION_STRING}")
    private String azureStorageConnectionString;

    @Autowired
    private LocalProcessLauncher localProcessLauncher;

    public void backupAzureWDS(String workspaceId, String backupName) throws IOException, InterruptedException {
        // TODO: Replace with application.properties value perhaps? Or a value from k8s?
        String containerName = "workspace-backups";
        Path backupDirectory = Paths.get("some_path");
        String blobName = workspaceId + "/" + backupName + ".sql";
        Map<String, String> pgDumpEnvVariables = new HashMap<>();

        pgDumpEnvVariables.put("PGHOST", "localhost");
        pgDumpEnvVariables.put("PGPORT", "5432");
        pgDumpEnvVariables.put("PGUSER", "myuser");
        pgDumpEnvVariables.put("PGPASSWORD", "mypassword");
        pgDumpEnvVariables.put("PGDATABASE", "mydatabase");

        // Build the pg_dump command
        List<String> command = List.of("pg_dump", "-U", "username", "-W", "password", "dbname");

        // Launch the process using LocalProcessLauncher
        localProcessLauncher.launchProcess(command, pgDumpEnvVariables, backupDirectory);

        // Create BlobServiceClient and BlobContainerClient
        BlobServiceClient blobServiceClient = new BlobServiceClientBuilder()
                .connectionString(azureStorageConnectionString)
                .buildClient();
        BlobContainerClient blobContainerClient = blobServiceClient.getBlobContainerClient(containerName);

        // Create the BlockBlobClient
        BlockBlobClient blockBlobClient = blobContainerClient.getBlobClient(blobName).getBlockBlobClient();

        // TODO: Upload the the streamed data to Azure Blob Storage
    }
}
