package org.databiosphere.workspacedataservice.controller;

import com.fasterxml.jackson.core.JsonProcessingException;
import org.databiosphere.workspacedataservice.service.AsyncService;
import org.databiosphere.workspacedataservice.service.model.SampleJob;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RestController;

/**
 * Proof-of-concept controller for asynchronous APIs
 */
@RestController
public class AsyncProofController {

    private final AsyncService asyncService;

    public AsyncProofController(AsyncService asyncService) {
        this.asyncService = asyncService;
    }

    /*
     * Start a new async SampleJob; return the job's id.
     */
    @PostMapping("/async")
    public ResponseEntity<SampleJob> startAsyncJob() {
        SampleJob newJobId = asyncService.startAsyncJob();
        return new ResponseEntity<>(newJobId, HttpStatus.CREATED);
    }

    /*
     * Get status for a SampleJob, given the job's id. Includes the job's result if the job is complete.
     */
    @GetMapping("/async/{jobId}")
    public ResponseEntity<SampleJob> describeAsyncJob(@PathVariable("jobId") String jobId) throws JsonProcessingException {
        SampleJob response = asyncService.describeAsyncJob(jobId);
        return new ResponseEntity<>(response, HttpStatus.OK);
    }
}