package org.databiosphere.workspacedataservice.service.model.exception;

import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.ResponseStatus;

import java.util.Arrays;

@ResponseStatus(HttpStatus.BAD_REQUEST)
public class MissingReferencedTableException extends RuntimeException {
    public MissingReferencedTableException(String[] missingTableNames) {
        super("Referenced table(s) " + Arrays.toString(missingTableNames) + " do(es) not exist");
    }
}
