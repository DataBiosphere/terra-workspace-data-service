package org.databiosphere.workspacedataservice.service;

import org.springframework.http.HttpStatus;
import org.springframework.web.server.ResponseStatusException;

public class RecordUtils {
    public static final String VERSION = "v0.2";

    public static void validateVersion(String version) {
        if (null == version || !version.equals(VERSION)) {
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST, "Invalid API version specified");
        }
    }
}
