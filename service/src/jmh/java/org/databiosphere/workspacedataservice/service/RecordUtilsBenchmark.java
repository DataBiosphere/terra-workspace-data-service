package org.databiosphere.workspacedataservice.service;

import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.Setup;


public class RecordUtilsBenchmark {

    @Setup

    @Benchmark
    public void validateVersion() {
        RecordUtils.validateVersion("v0.2");
    }

}
