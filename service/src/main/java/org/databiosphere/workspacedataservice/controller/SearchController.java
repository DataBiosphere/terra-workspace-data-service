package org.databiosphere.workspacedataservice.controller;

import org.databiosphere.workspacedataservice.service.OpenSearchService;
import org.databiosphere.workspacedataservice.shared.model.RecordQueryResponse;
import org.databiosphere.workspacedataservice.shared.model.RecordType;
import org.databiosphere.workspacedataservice.shared.model.SearchRequest;
import org.databiosphere.workspacedataservice.shared.model.TsvUploadResponse;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;
import java.util.UUID;

@RestController
public class SearchController {

	private final OpenSearchService openSearchService;

	public SearchController(OpenSearchService openSearchService) {
		this.openSearchService = openSearchService;
	}

	@PostMapping("/{instanceid}/opensearch/{version}/search/{recordType}")
	public RecordQueryResponse search(@PathVariable("instanceid") UUID instanceId,
			@PathVariable("recordType") RecordType recordType,
			@PathVariable("version") String version,
			@RequestBody(required = false) SearchRequest searchRequest) {


		return openSearchService.queryForRecords(instanceId, recordType, version, searchRequest);
	}

	@PostMapping("/{instanceid}/opensearch/{version}/index/{recordType}")
	public ResponseEntity<TsvUploadResponse>  reindex(@PathVariable("instanceid") UUID instanceId,
									  @PathVariable("recordType") RecordType recordType,
									  @PathVariable("version") String version) {

		TsvUploadResponse bulkResponse = openSearchService.reindex(instanceId, recordType);

		return new ResponseEntity<>(bulkResponse, HttpStatus.OK);
	}

	@DeleteMapping("/{instanceid}/opensearch/{version}/index/{recordType}")
	public ResponseEntity<TsvUploadResponse>  unindex(@PathVariable("instanceid") UUID instanceId,
													  @PathVariable("recordType") RecordType recordType,
													  @PathVariable("version") String version) {

		TsvUploadResponse bulkResponse = openSearchService.unindex(instanceId, recordType);

		return new ResponseEntity<>(bulkResponse, HttpStatus.OK);
	}

	@DeleteMapping("/{instanceid}/opensearch/{version}")
	public ResponseEntity<String> deleteOpenSearchIndex(@PathVariable("instanceid") UUID instanceId,
												 @PathVariable("version") String version) {
		boolean ack = openSearchService.deleteIndex(instanceId);
		HttpStatus status = ack ? HttpStatus.OK : HttpStatus.INTERNAL_SERVER_ERROR;
		return new ResponseEntity<>(status);
	}

	@PostMapping("/{instanceid}/opensearch/{version}")
	public ResponseEntity<String> createOpenSearchIndex(@PathVariable("instanceid") UUID instanceId,
														@PathVariable("version") String version) {
		boolean ack = openSearchService.createIndex(instanceId);
		HttpStatus status = ack ? HttpStatus.OK : HttpStatus.INTERNAL_SERVER_ERROR;
		return new ResponseEntity<>(status);
	}

}
