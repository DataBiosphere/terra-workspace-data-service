package org.databiosphere.workspacedataservice.service;

import bio.terra.common.db.WriteTransaction;
import org.databiosphere.workspacedataservice.InstanceInitializerBean;
import org.databiosphere.workspacedataservice.process.LocalProcessLauncher;
import com.azure.storage.blob.BlobContainerClient;
import com.azure.storage.blob.BlobServiceClient;
import com.azure.storage.blob.BlobServiceClientBuilder;
import com.azure.storage.blob.specialized.BlockBlobClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.io.InputStream;
import java.util.List;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.UUID;

@Service
public class BackupService {

    private static final Logger LOGGER = LoggerFactory.getLogger(BackupService.class);

    @Autowired
    private LocalProcessLauncher localProcessLauncher;

    @WriteTransaction
    public void backupAzureWDS(UUID workspaceId) {
        String backupName = "some-backup";
        String blobName = workspaceId.toString() + "-" + backupName + ".sql";
        Path backupDirectory = Paths.get("some_path");

        List<String> command = List.of(
                "pg_dump",
                "-h", System.getenv("WDS_DB_HOST"),
                "-p", System.getenv("WDS_DB_PORT"),
                "-U", System.getenv("WDS_DB_USER"),
                "-d", System.getenv("WDS_DB_NAME"),
                "-W", System.getenv("WDS_DB_PASSWORD")
        );

        InputStream pgDumpOutput = localProcessLauncher.launchProcess(command, null, null);

        BlockBlobClient blockBlobClient = constructBlockBlobClient(blobName);
        // -1 represents using the default parallelTransferOptions during upload to Azure
        // From docs, this means each block size: 4 MB (4 * 1024 * 1024 bytes), maximum number of parallel transfers: 2
        blockBlobClient.upload(pgDumpOutput, -1);

    }

    public BlockBlobClient constructBlockBlobClient(String blobName) {
        // TODO: Replace with application.properties value perhaps? Or a value from k8s?
        String containerName = "workspace-backups";

        // Example Azure Postgres Connection String
        String azureStorageConnectionString = "jdbc:postgresql://" + System.getenv("WDS_DB_HOST")
                + "postgres.database.azure.com:" + System.getenv("WDS_DB_PORT") + "/" + System.getenv("WDS_DB_NAME")
                + "?user=" + System.getenv("WDS_DB_USER") + "@" + System.getenv("WDS_DB_HOST") + "&password="
                + System.getenv("WDS_DB_PASSWORD") + "&sslmode=require";

        BlobServiceClient blobServiceClient = new BlobServiceClientBuilder()
                .connectionString(azureStorageConnectionString)
                .buildClient();
        BlobContainerClient blobContainerClient = blobServiceClient.getBlobContainerClient(containerName);

        return blobContainerClient.getBlobClient(blobName).getBlockBlobClient();
    }
}
