package org.databiosphere.workspacedataservice.storage;

import org.databiosphere.workspacedataservice.service.model.exception.LaunchProcessException;

import java.io.*;
import java.nio.charset.StandardCharsets;

public class LocalFileStorage implements BackUpFileStorage {
    public LocalFileStorage() {}

    public void streamOutputToBlobStorage(InputStream fromStream, String blobName) {
        File targetFile = new File("backup.sql");
        try(OutputStream outStream = new FileOutputStream(targetFile)) {
            try (BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(fromStream, StandardCharsets.UTF_8))) {
                int line;
                while ((line = bufferedReader.read()) != -1) {
                    outStream.write(line);
                }
            }
        } catch (IOException ioEx) {
            throw new LaunchProcessException("Error streaming output during local test", ioEx);
        }
    }

    public void streamInputFromBlobStorage(OutputStream toStream, String blobName) {
        File targetFile = new File("backup.sql");
        try(InputStream inStream = new FileInputStream(targetFile)) {
            try(BufferedWriter bufferedWriter = new BufferedWriter(new OutputStreamWriter(toStream, StandardCharsets.UTF_8))) {
                int line;
                while((line = inStream.read()) != -1) {
                    bufferedWriter.write((line));
                }
            }
        } catch (IOException ioEx) {
            throw new LaunchProcessException("Error streaming input during local test", ioEx);
        }
    }
}
