package org.databiosphere.workspacedataservice.service;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Map;
import java.util.HashMap;
import java.util.Scanner;
import java.io.File;

import com.fasterxml.jackson.databind.ObjectMapper;

public class WDSVersionInfo {
    /**
     * WDSVersionInfo is a utility class that parses a VERSION.txt file into a valid payload for a ResponseEntity.
     *
     * The VERSION.txt file contains relevant version metadata for WDS. Via GitHub Actions, VERSION.txt
     * is updated upon each merge in the `main` branch of WDS.
     */

    // Working directory when invoked in API is terra-workspace-data-service/service/
    private static final String VERSION_FILE_LOCATION = "VERSION.json";

    public Map<String, String> getWDSVersionInfo() throws IOException {

        ObjectMapper mapper = new ObjectMapper();
        Map<String,String> versionDetailsMap = mapper.readValue(new File(VERSION_FILE_LOCATION), Map.class);
        return versionDetailsMap;

    }

}