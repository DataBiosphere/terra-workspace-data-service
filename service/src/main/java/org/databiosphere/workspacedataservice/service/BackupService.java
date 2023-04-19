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

    public void backupAzureWDS(String workspaceId, String backupName) {
        // TODO: Replace with application.properties value perhaps? Or a value from k8s?
        String containerName = "workspace-backups";
        Path backupDirectory = Paths.get("some_path");
        String blobName = workspaceId + "/" + backupName + ".sql";

        // Build the pg_dump command
        List<String> command = List.of(
                "pg_dump",
                "-h", System.getenv("WDS_DB_HOST"),
                "-p", System.getenv("WDS_DB_PORT"),
                "-U", System.getenv("WDS_DB_USER"),
                "-d", System.getenv("WDS_DB_NAME"),
                "-W", System.getenv("WDS_DB_PASSWORD"),
                "-F", "c", // "c" represents "compressed" format
        );


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
