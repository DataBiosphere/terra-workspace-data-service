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
import org.springframework.context.annotation.Configuration;

import java.io.*;
import java.nio.charset.StandardCharsets;

@Configuration
public class AzureBlobStorage implements BackUpFileStorage {

    private final WorkspaceManagerDao workspaceManagerDao;

    public AzureBlobStorage(WorkspaceManagerDao workspaceManagerDao) {
        this.workspaceManagerDao = workspaceManagerDao;
    }

    private static final Logger LOGGER = LoggerFactory.getLogger(AzureBlobStorage.class);

    /*
    6/28/2023 - Terra's storage inside a billing project is organized as follows:
    by default a billing project gets a single blob storage azure resource
    each workspace that gets created inside of that billing project will get its own container inside the blob storage
    the container will follow the naming of "sc-<workspaceId>"
    all files that are uploaded into the workspace will be inside the appropriate workspace container
    containers are restricted to have the same access as the users who have access to the workspace

    backups will be stored inside the workspace container in a path determined by function GenerateBackupFilename
    which exists within BackupService.java.
     */
    @Override
    public void streamOutputToBlobStorage(InputStream fromStream, String blobName, String workspaceId) {
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

    @Override
    public void streamInputFromBlobStorage(OutputStream toStream, String blobName, String workspaceId) {
        BlobContainerClient blobContainerClient = constructBlockBlobClient(workspaceId);
        LOGGER.info("Blob storage container client was found. ");
        try (toStream) {
            blobContainerClient.getBlobClient(blobName).downloadStream(toStream);
        } catch (IOException ioEx) {
            throw new LaunchProcessException("Error streaming input to child process", ioEx);
        }
    }

    public BlobContainerClient constructBlockBlobClient(String workspaceId) {
        // get workspace blob storage endpoint and token
        var blobstorageDetails = workspaceManagerDao.getBlobStorageUrl(workspaceId);

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

    public void DeleteBlob(String blobFile, String workspaceId) {
        BlobContainerClient blobContainerClient = constructBlockBlobClient(workspaceId);

        try {
            var blobClient = blobContainerClient.getBlobClient(blobFile);
            blobClient.delete();
        }
        catch (BlobStorageException e){
            LOGGER.error("Failed to delete file with name {}. ", blobFile);
            throw(e);
        }
    }
}