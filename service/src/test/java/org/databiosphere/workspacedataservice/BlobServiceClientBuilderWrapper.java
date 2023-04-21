package org.databiosphere.workspacedataservice;

import com.azure.storage.blob.BlobServiceClient;
import com.azure.storage.blob.BlobServiceClientBuilder;

public class BlobServiceClientBuilderWrapper  {

    private final BlobServiceClientBuilder builder;

    public BlobServiceClientBuilderWrapper() {
        this.builder = new BlobServiceClientBuilder();
    }

    public BlobServiceClientBuilderWrapper connectionString(String connectionString) {
        builder.connectionString(connectionString);
        return this;
    }

    public BlobServiceClient buildClient() {
        return builder.buildClient();
    }
}