package org.databiosphere.workspacedataservice.storage;

import com.azure.storage.blob.BlobContainerClient;
import com.azure.storage.blob.BlobServiceClient;
import com.azure.storage.blob.BlobServiceClientBuilder;
import com.azure.storage.blob.models.BlobStorageException;
import com.azure.storage.blob.specialized.BlobOutputStream;
import org.databiosphere.workspacedataservice.service.model.exception.LaunchProcessException;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;

public class AzureBlobStorage implements BackUpFileStorage {
    private static String backUpContainerName = "backup";
    public AzureBlobStorage() {}
    @Override
    public void streamOutputToBlobStorage(InputStream fromStream, String blobName) {
        // TODO: remove this once connection is switched to be done via SAS token
        String storageConnectionString = System.getenv("STORAGE_CONNECTION_STRING");
        BlobContainerClient blobContainerClient = constructBlockBlobClient(backUpContainerName, storageConnectionString);

        // https://learn.microsoft.com/en-us/java/api/overview/azure/storage-blob-readme?view=azure-java-stable#upload-a-blob-via-an-outputstream
        try (BlobOutputStream blobOS = blobContainerClient.getBlobClient(blobName).getBlockBlobClient().getBlobOutputStream()) {
            try (BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(fromStream, StandardCharsets.UTF_8))) {
                int line;
                while ((line = bufferedReader.read()) != -1) {
                    blobOS.write(line);
                }
            }
        } catch (IOException ioEx) {
            throw new LaunchProcessException("Error streaming output of child process", ioEx);
        }
    }

    public static BlobContainerClient constructBlockBlobClient(String containerName, String connectionString) {
        BlobServiceClient blobServiceClient = new BlobServiceClientBuilder()
                .connectionString(connectionString)
                .buildClient();

        // TODO: this will be used when connection to blob storage will be done via SAS token vs connection string
        //BlobServiceClient storageClient = new BlobServiceClientBuilder().endpoint(endpoint).sasToken(token).buildClient();

        // if the backup container in storage doesnt already exists, it will need to be created
        try {
            return blobServiceClient.getBlobContainerClient(containerName);
        }
        catch (BlobStorageException e){
            return blobServiceClient.createBlobContainerIfNotExists(containerName);
        }
    }
}
