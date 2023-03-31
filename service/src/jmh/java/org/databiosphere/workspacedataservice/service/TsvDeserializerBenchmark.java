package org.databiosphere.workspacedataservice.service;

import org.databiosphere.workspacedataservice.WorkspaceDataServiceApplication;
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

@State(value = Scope.Benchmark)
public class TsvDeserializerBenchmark {

    private final Logger logger = LoggerFactory.getLogger(getClass());

    static ConfigurableApplicationContext context;
    private DataTypeInferer dataTypeInferer;
    private TsvDeserializer tsvDeserializer;

    /**
     * Setup: boot up the Spring context for WorkspaceDataServiceApplication.
     * From that context, grab the beans we want to test.
     */
    @Setup(Level.Trial)
    public synchronized void initialize() {
        try {
            String args = "--spring.main.allow-bean-definition-overriding=true";
            if(context == null) {
                context = SpringApplication.run(WorkspaceDataServiceApplication.class, args);
            }
            dataTypeInferer = context.getBean(DataTypeInferer.class);
            tsvDeserializer = context.getBean(TsvDeserializer.class);
        } catch(Exception e) {
            logger.error("failed to boot Spring context: " + e.getMessage(), e);
        }
    }

    @TearDown
    public void tearDown() {
        SpringApplication.exit(context, () -> 0); // exit with code 0
    }

    // to benchmark:
    // - stringified json objects
    // - stringified dates, datetimes
    // - stringified file references
    // - stringified booleans

    // test cases
    private final String jsonString = "{\"foo\":\"bar\"}";
    private final String booleanString = "true";
    private final String dateString = "2021-10-03";
    private final String datetimeString = "2021-10-03T19:01:23";
    private final String fileString = "https://lz1a2b345c67def8a91234bc.blob.core.windows.net/sc-7ad51c5d-eb4c-4685-bffe-62b861f7753f/my%20file.pdf";
    private final String stringString = "Hello world";


    @Benchmark
    public void cellToAttributeJson() {
        tsvDeserializer.cellToAttribute(jsonString);
    }

    @Benchmark
    public void inferTypeJson() {
        dataTypeInferer.inferType(jsonString);
    }

//    @Benchmark
    public void cellToAttributeBoolean() {
        tsvDeserializer.cellToAttribute(booleanString);
    }

//    @Benchmark
    public void inferTypeBoolean() {
        dataTypeInferer.inferType(booleanString);
    }

//    @Benchmark
    public void cellToAttributeDate() {
        tsvDeserializer.cellToAttribute(dateString);
    }

//    @Benchmark
    public void inferTypeDate() {
        dataTypeInferer.inferType(dateString);
    }

//    @Benchmark
    public void cellToAttributeDatetime() {
        tsvDeserializer.cellToAttribute(datetimeString);
    }

//    @Benchmark
    public void inferTypeDatetime() {
        dataTypeInferer.inferType(datetimeString);
    }

//    @Benchmark
    public void cellToAttributeFile() {
        tsvDeserializer.cellToAttribute(fileString);
    }

//    @Benchmark
    public void inferTypeFile() {
        dataTypeInferer.inferType(fileString);
    }

//    @Benchmark
    public void cellToAttributeString() {
        tsvDeserializer.cellToAttribute(stringString);
    }

//    @Benchmark
    public void inferTypeString() {
        dataTypeInferer.inferType(stringString);
    }


}
