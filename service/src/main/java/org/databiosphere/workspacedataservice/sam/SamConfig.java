package org.databiosphere.workspacedataservice.sam;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.UUID;

/**
 * Bean creator for:
 * - SamClientFactory, injecting the base url to Sam into that factory.
 * - SamDao, injecting the SamClientFactory into that dao.
 */
@Configuration
public class SamConfig {

    @Value("${SAM_URL:}")
    private String samUrl;

    @Value("${sam.enabled:true}")
    private boolean isSamEnabled;

    @Value("${twds.instance.workspace-id:}")
    private String workspaceIdArgument;

    private static final Logger LOGGER = LoggerFactory.getLogger(SamConfig.class);

    @Bean HttpSamClientSupport getHttpSamClientSupport() {
        return new HttpSamClientSupport();
    }

    @Bean
    public SamClientFactory getSamClientFactory() {
        // TODO: AJ-898 what validation of the sam url should we do here?
        // - none
        // - check if the value is null/empty/whitespace
        // - check if the value is a valid Url
        // - contact the url and see if it looks like Sam on the other end
        // TODO: AJ-898 and what should we do if the validation fails?
        // - nothing, which would almost certainly result in Sam calls failing
        // - disable Sam integration, which could result in unauthorized access
        // - stop WDS, which would obviously prevent WDS from working at all
        LOGGER.info("Using Sam base url: '{}'", samUrl);
        if (isSamEnabled) {
            LOGGER.info("Sam integration enabled.");
        } else {
            LOGGER.warn("Sam integration disabled via sam.enabled property. " +
                    "All Sam calls will return true/successful but will not connect to Sam.");
        }
        return new HttpSamClientFactory(samUrl, isSamEnabled);
    }

    @Bean
    public SamDao samDao(SamClientFactory samClientFactory, HttpSamClientSupport httpSamClientSupport) {
        try {
            String workspaceId = UUID.fromString(workspaceIdArgument).toString(); // verify UUID-ness
            LOGGER.info("Sam integration will query type={}, resourceId={}, action={}",
                    SamDao.RESOURCE_NAME_WORKSPACE, workspaceId, SamDao.ACTION_WRITE);
            return new HttpSamDao(samClientFactory, httpSamClientSupport, workspaceId);
        } catch (IllegalArgumentException e) {
            LOGGER.warn("Workspace id could not be parsed, all Sam permission checks will fail. Provided id: {}", workspaceIdArgument);
            return new FailingSamDao("WDS was started with invalid WORKSPACE_ID of: " + workspaceIdArgument);
        } catch (Exception e) {
            LOGGER.warn("Error during initial Sam configuration: " + e.getMessage());
            return new FailingSamDao(e.getMessage());
        }
    }

}
