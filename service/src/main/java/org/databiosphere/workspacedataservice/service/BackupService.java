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

import java.io.*;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.stream.Collectors;

@Service
public class BackupService {

    private static final Logger LOGGER = LoggerFactory.getLogger(BackupService.class);

    @Autowired
    private LocalProcessLauncher localProcessLauncher;

    @WriteTransaction
    public void backupAzureWDS(UUID workspaceId) throws Exception {

        LocalDateTime now = LocalDateTime.now();
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd_HH-mm-ss");
        String timestamp = now.format(formatter);
        String blobName = workspaceId.toString() + "-" + timestamp + ".sql";

//        String dbHost = System.getenv("WDS_DB_HOST");
//        String dbPort = System.getenv("WDS_DB_PORT");
//        String dbUser = System.getenv("WDS_DB_USER");
//        String dbName = System.getenv("WDS_DB_NAME");
//        String dbPassword = System.getenv("WDS_DB_PASSWORD");
        String dbHost = "0.0.0.0";
        String dbPort = "5432";
        String dbUser = "postgres";
        String dbName = "wds";
        String dbPassword = "postgres";

        Map<String, String> command = new LinkedHashMap<>();
        command.put("pg_dump", null);
        command.put("-h", dbHost);
        command.put("-p", dbPort);
        command.put("-U", dbUser);
        command.put("-d", dbName);
//        command.put("--no-owner", null);
//        command.put("--no-acl", null);

        List<String> commandList = new ArrayList<>();
        for (Map.Entry<String, String> entry : command.entrySet()) {
            commandList.add(entry.getKey());
            if (entry.getValue() != null) {
                commandList.add(entry.getValue());
            }
        }
        commandList.add("-v");
        commandList.add("-w");
        System.out.println(commandList);

        InputStream inputStream = localProcessLauncher.launchProcess(commandList, dbPassword);
        System.out.println(localProcessLauncher.getOutputForProcessFromStream(inputStream));

//        BlockBlobClient blockBlobClient = constructBlockBlobClient(workspaceId.toString() + "-backups", blobName);
//        // -1 represents using the default parallelTransferOptions during upload to Azure
//        // From docs, this means each block size: 4 MB (4 * 1024 * 1024 bytes), maximum number of parallel transfers: 2
//        blockBlobClient.upload(pgDumpOutput, -1);

    }

    public BlockBlobClient constructBlockBlobClient(String containerName, String blobName) {
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
