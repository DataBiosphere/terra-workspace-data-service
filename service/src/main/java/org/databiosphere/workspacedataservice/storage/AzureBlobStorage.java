package org.databiosphere.workspacedataservice.storage;

import com.azure.storage.blob.BlobContainerClient;
import com.azure.storage.blob.BlobServiceClient;
import com.azure.storage.blob.BlobServiceClientBuilder;
import com.azure.storage.blob.models.BlobStorageException;
import com.azure.storage.blob.specialized.BlobOutputStream;
import org.databiosphere.workspacedataservice.service.model.exception.LaunchProcessException;
import org.databiosphere.workspacedataservice.workspacemanager.WorkspaceManagerDao;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;

public class AzureBlobStorage implements BackUpFileStorage {
    private final WorkspaceManagerDao workspaceManagerDao;
    public AzureBlobStorage(WorkspaceManagerDao workspaceManagerDao) {
        this.workspaceManagerDao = workspaceManagerDao;
    }
    private static final Logger LOGGER = LoggerFactory.getLogger(AzureBlobStorage.class);

    @Override
    public void streamOutputToBlobStorage(InputStream fromStream, String blobName, String workspaceId) {
        // TODO: remove this once connection is switched to be done via SAS token
        LOGGER.info("Creating blob storage client. ");
        BlobContainerClient blobContainerClient = constructBlockBlobClient(workspaceId);
        LOGGER.info("About to write to blob storage. ");
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

    public BlobContainerClient constructBlockBlobClient(String workspaceId) {
        // get  workspace blob storage endpoint and token
        var blobstorageDetails = workspaceManagerDao.getBlobStorageUrl();

        // the url we get from WSM already contains the token in it, so no need to specify sasToken separately
        BlobServiceClient blobServiceClient = new BlobServiceClientBuilder().endpoint(blobstorageDetails).buildClient();

        try {
            // the way storage containers are set up in a workspace are as follows:
            // billing project gets a single azure storage account
            // each workspace gets a container inside of that storage account to keep its data
            return blobServiceClient.getBlobContainerClient("sc-"+ workspaceId);
        }
        catch (BlobStorageException e){
            // if the default workspace container doesn't exist, something went horribly wrong
            LOGGER.error("Default storage container missing for workspace id {}", workspaceId);
            throw(e);
        }
    }
}
