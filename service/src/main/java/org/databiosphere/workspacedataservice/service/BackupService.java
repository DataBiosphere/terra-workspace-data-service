package org.databiosphere.workspacedataservice.service;

import org.databiosphere.workspacedataservice.process.LocalProcessLauncher;
import com.azure.storage.blob.BlobContainerClient;
import com.azure.storage.blob.BlobServiceClient;
import com.azure.storage.blob.BlobServiceClientBuilder;
import com.azure.storage.blob.specialized.BlockBlobClient;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.io.InputStream;
import java.util.List;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.UUID;

@Service
public class BackupService {

    @Autowired
    private LocalProcessLauncher localProcessLauncher;

    public void backupAzureWDS(UUID instanceId, UUID workspaceId) {
        String backupName = "goodbye";
        String blobName = instanceId.toString() + "/" + workspaceId.toString() + "/" + backupName + ".sql";
        Path backupDirectory = Paths.get("some_path");

        List<String> command = List.of(
                "pg_dump",
                "-h", System.getenv("WDS_DB_HOST"),
                "-p", System.getenv("WDS_DB_PORT"),
                "-U", System.getenv("WDS_DB_USER"),
                "-d", System.getenv("WDS_DB_NAME"),
                "-W", System.getenv("WDS_DB_PASSWORD")
        );

        InputStream pgDumpOutput = localProcessLauncher.launchProcess(command, null, backupDirectory);

        BlockBlobClient blockBlobClient = constructBlockBlobClient(blobName);
        // -1 represents using the default parallelTransferOptions during upload to Azure
        // From docs, this means each block size: 4 MB (4 * 1024 * 1024 bytes), maximum number of parallel transfers: 2
        blockBlobClient.upload(pgDumpOutput, -1);
    }

    public BlockBlobClient constructBlockBlobClient(String blobName) {
        // TODO: Replace with application.properties value perhaps? Or a value from k8s?
        String containerName = "workspace-backups";
        String azureStorageConnectionString = "something-else";

        BlobServiceClient blobServiceClient = new BlobServiceClientBuilder()
                .connectionString(azureStorageConnectionString)
                .buildClient();
        BlobContainerClient blobContainerClient = blobServiceClient.getBlobContainerClient(containerName);

        return blobContainerClient.getBlobClient(blobName).getBlockBlobClient();
    }
}
