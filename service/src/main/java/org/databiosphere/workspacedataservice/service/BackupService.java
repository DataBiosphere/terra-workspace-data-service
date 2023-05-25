package org.databiosphere.workspacedataservice.service;

import bio.terra.common.db.WriteTransaction;
import org.apache.commons.lang3.StringUtils;
import org.databiosphere.workspacedataservice.InstanceInitializerBean;
import org.databiosphere.workspacedataservice.process.LocalProcessLauncher;
import com.azure.storage.blob.BlobContainerClient;
import com.azure.storage.blob.BlobServiceClient;
import com.azure.storage.blob.BlobServiceClientBuilder;
import com.azure.storage.blob.specialized.BlockBlobClient;
import com.azure.storage.blob.specialized.BlobOutputStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.io.*;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;

import org.databiosphere.workspacedataservice.service.model.exception.LaunchProcessException;
import java.nio.charset.StandardCharsets;

@Service
public class BackupService {

    private static final Logger LOGGER = LoggerFactory.getLogger(BackupService.class);

    @WriteTransaction
    public void backupAzureWDS(UUID workspaceId) throws Exception {
        LocalDateTime now = LocalDateTime.now();
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd_HH-mm-ss");
        String timestamp = now.format(formatter);
        String blobName = workspaceId.toString() + "-" + timestamp + ".sql";

        String dbHost = System.getenv("WDS_DB_HOST");
        String dbPort = System.getenv("WDS_DB_PORT");
        String dbUser = System.getenv("WDS_DB_USER");
        String dbName = System.getenv("WDS_DB_NAME");
        String dbPassword = System.getenv("WDS_DB_PASSWORD");

        Map<String, String> command = new LinkedHashMap<>();
        command.put("/usr/bin/pg_dump", null);
        command.put("-h", dbHost);
        command.put("-p", dbPort);
        command.put("-U", dbUser);
        command.put("-d", dbName);

        List<String> commandList = new ArrayList<>();
        for (Map.Entry<String, String> entry : command.entrySet()) {
            commandList.add(entry.getKey());
            if (entry.getValue() != null) {
                commandList.add(entry.getValue());
            }
        }
        commandList.add("-v");
        commandList.add("-w");
        LOGGER.info("process command list: " + commandList);

        Map<String, String> envVars = Map.of("PGPASSWORD", dbPassword);

        LocalProcessLauncher localProcessLauncher = new LocalProcessLauncher();
        localProcessLauncher.launchProcess(commandList, envVars);
        streamOutputToBlobStorage(localProcessLauncher.getInputStream(), blobName);

        String error = localProcessLauncher.getOutputForProcess(LocalProcessLauncher.Output.ERROR);
        int exitCode = localProcessLauncher.waitForTerminate();

        if (exitCode != 0 && StringUtils.isNotBlank(error)) {
            LOGGER.error("process error: " + error);
        }
    }

    private static void streamOutputToBlobStorage(InputStream fromStream, String blobName) {
        // TODO: remove this once connection is switched to be done via SAS token
        String storageConnectionString = System.getenv("STORAGE_CONNECTION_STRING");
        BlobContainerClient blobContainerClient = constructBlockBlobClient("backup", storageConnectionString);

        // TODO: call function that generates a name for the backup
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
        //BlobServiceClient storageClient = new BlobServiceClientBuilder().endpoint(endpoint).credential(credential).buildClient();

        return blobServiceClient.getBlobContainerClient(containerName);
    }
}
