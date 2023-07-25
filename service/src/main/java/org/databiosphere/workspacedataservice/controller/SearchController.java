package org.databiosphere.workspacedataservice.controller;

import org.databiosphere.workspacedataservice.dao.OpenSearchDao;
import org.databiosphere.workspacedataservice.retry.RetryableApi;
import org.databiosphere.workspacedataservice.service.InstanceService;
import org.databiosphere.workspacedataservice.service.RecordOrchestratorService;
import org.databiosphere.workspacedataservice.shared.model.RecordQueryResponse;
import org.databiosphere.workspacedataservice.shared.model.RecordType;
import org.databiosphere.workspacedataservice.shared.model.SearchRequest;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;
import java.util.UUID;

@RestController
public class SearchController {

	private final InstanceService instanceService;
	private final RecordOrchestratorService recordOrchestratorService;

	private final OpenSearchDao openSearchDao;

	public SearchController(InstanceService instanceService, RecordOrchestratorService recordOrchestratorService, OpenSearchDao openSearchDao) {
		this.instanceService = instanceService;
		this.recordOrchestratorService = recordOrchestratorService;
		this.openSearchDao = openSearchDao;
	}

	@PostMapping("/{instanceid}/opensearch/{version}/{recordType}")
	@RetryableApi
	public RecordQueryResponse search(@PathVariable("instanceid") UUID instanceId,
			@PathVariable("recordType") RecordType recordType,
			@PathVariable("version") String version,
			@RequestBody(required = false) SearchRequest searchRequest) {

		int theCount = Long.valueOf(openSearchDao.count()).intValue();
		return new RecordQueryResponse(searchRequest, List.of(), theCount);
	}

}
