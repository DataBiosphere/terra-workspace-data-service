package org.databiosphere.workspacedataservice.controller;

import org.databiosphere.workspacedataservice.WorkspaceDataServiceApplication;
import org.databiosphere.workspacedataservice.service.DataTypeInferer;
import org.databiosphere.workspacedataservice.shared.model.RecordType;
import org.databiosphere.workspacedataservice.tsv.TsvDeserializer;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.SpringApplication;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.http.MediaType;
import org.springframework.mock.web.MockMultipartFile;
import org.springframework.web.multipart.MultipartFile;

import java.io.IOException;
import java.util.Optional;
import java.util.UUID;

@State(value = Scope.Benchmark)
public class ApiBenchmark {

    private final Logger logger = LoggerFactory.getLogger(getClass());

    static ConfigurableApplicationContext context;
    private RecordController recordController;
    private MultipartFile tsvFile;

    private final String VERSION = "v0.2";
    private final UUID instanceId = UUID.randomUUID();

    /**
     * Setup: boot up the Spring context for WorkspaceDataServiceApplication.
     * From that context, grab the beans we want to test.
     */
    @Setup(Level.Trial)
    public synchronized void initialize() throws IOException {
        // start up the Spring context and grab the controller bean
        try {
            String args = "--spring.main.allow-bean-definition-overriding=true";
            if(context == null) {
                context = SpringApplication.run(WorkspaceDataServiceApplication.class, args);
            }
            recordController = context.getBean(RecordController.class);
        } catch(Exception e) {
            logger.error("failed to boot Spring context: " + e.getMessage(), e);
        }
        // load the TSV file
        tsvFile = new MockMultipartFile("records", "test.tsv", MediaType.TEXT_PLAIN_VALUE,
                ApiBenchmark.class.getResourceAsStream("/upload.tsv"));
        // create instance
        recordController.createInstance(instanceId, VERSION, Optional.empty());
    }

    @TearDown
    public void tearDown() {
        SpringApplication.exit(context, () -> 0); // exit with code 0
    }

    @Benchmark
    public void uploadTsv() throws IOException {
        recordController.tsvUpload(instanceId, VERSION,
                RecordType.valueOf("my-record-type"),
                Optional.of("my-primary-key"),
                tsvFile);
    }

}
