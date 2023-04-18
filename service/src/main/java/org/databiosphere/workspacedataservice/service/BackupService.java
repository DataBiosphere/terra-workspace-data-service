package org.databiosphere.workspacedataservice.service;

import com.azure.storage.blob.BlobClient;
import com.azure.storage.blob.BlobContainerClient;
import com.azure.storage.blob.BlobServiceClient;
import com.azure.storage.blob.BlobServiceClientBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.UUID;

@Service
public class BackupService {

    private static final Logger LOGGER = LoggerFactory.getLogger(BackupService.class);

    public void backupPostgresToAzure(String connectionString, String containerName,
                                      String postgresUser, String postgresPassword, String dbName, UUID workspaceId) {
        try {
            // Temp. file to store the pg_dump output
            Path tempFile = Files.createTempFile("pg_backup_", ".sql");
            File backupFile = tempFile.toFile();

            // pg_dump command
            ProcessBuilder processBuilder = new ProcessBuilder(
                    "pg_dump",
                    "--username=" + postgresUser,
                    "--password=" + postgresPassword,
                    "--file=" + backupFile.getAbsolutePath(),
                    dbName
            );
            Process process = processBuilder.start();

            int exitCode = process.waitFor();
            if (exitCode == 0) {

                // Upload the backup file to Azure Blob Storage
                BlobClient blobClient = getAzureBlobClient(connectionString, containerName, workspaceId);
                try (FileInputStream backupInputStream = new FileInputStream(backupFile)) {
                    blobClient.upload(backupInputStream, backupFile.length());
                }

                // Delete temp. file
                Files.delete(tempFile);
            } else {
                LOGGER.warn("pg_dump failed with exit code: {} for workspace Id: {}", exitCode, workspaceId);
                System.err.println("pg_dump failed with exit code: " + exitCode);
                BufferedReader errorReader = new BufferedReader(new InputStreamReader(process.getErrorStream()));
                String line;
                while ((line = errorReader.readLine()) != null) {
                    LOGGER.warn("Error output for workspace Id: {} -- output: {}", workspaceId, line);
                }
            }
        } catch (IOException | InterruptedException e) {
            LOGGER.warn("Exception occurred for pg_dump for workspace Id: {} -- error: {}", workspaceId, e);
        }
    }

    private BlobClient getAzureBlobClient(String connectionString, String containerName, UUID workspaceId) {
        // Create a BlobServiceClient and container client
        BlobServiceClient blobServiceClient = new BlobServiceClientBuilder().connectionString(connectionString).buildClient();
        BlobContainerClient containerClient = blobServiceClient.getBlobContainerClient(containerName);
        return containerClient.getBlobClient(constructBackupBlobName(workspaceId));
    }

    private String constructBackupBlobName(UUID workspaceId) {
        LocalDateTime now = LocalDateTime.now();

        // Make sure backup name is unique
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyyMMddHHmmss");
        return "pg_backup_" + workspaceId.toString() + "_" + now.format(formatter) + ".sql";
    }

}
