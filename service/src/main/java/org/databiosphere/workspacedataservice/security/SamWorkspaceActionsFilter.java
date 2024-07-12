package org.databiosphere.workspacedataservice.security;

import jakarta.servlet.FilterChain;
import jakarta.servlet.ServletException;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.util.List;
import java.util.UUID;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.databiosphere.workspacedataservice.sam.SamAuthorizationDao;
import org.databiosphere.workspacedataservice.sam.SamAuthorizationDaoFactory;
import org.databiosphere.workspacedataservice.service.CollectionService;
import org.databiosphere.workspacedataservice.service.JobService;
import org.databiosphere.workspacedataservice.shared.model.CollectionId;
import org.databiosphere.workspacedataservice.shared.model.WorkspaceId;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.security.core.authority.SimpleGrantedAuthority;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.web.filter.OncePerRequestFilter;

public class SamWorkspaceActionsFilter extends OncePerRequestFilter {

  private final SamAuthorizationDaoFactory samAuthorizationDaoFactory;
  private final CollectionService collectionService;
  private final JobService jobService;

  private static final Logger logger = LoggerFactory.getLogger(SamWorkspaceActionsFilter.class);

  public SamWorkspaceActionsFilter(
      SamAuthorizationDaoFactory samAuthorizationDaoFactory,
      CollectionService collectionService,
      JobService jobService) {
    this.samAuthorizationDaoFactory = samAuthorizationDaoFactory;
    this.collectionService = collectionService;
    this.jobService = jobService;
  }

  @Override
  protected void doFilterInternal(
      @NotNull HttpServletRequest request,
      @NotNull HttpServletResponse response,
      @NotNull FilterChain filterChain)
      throws ServletException, IOException {

    // Extract the collectionId, workspaceId, or jobId from the request path
    // Resolve collectionId/jobId to its corresponding workspaceId
    WorkspaceId workspaceId = determineWorkspaceId(request);

    // if we didn't find a workspace id, stop here
    if (workspaceId == null) {
      logger.info("********** no workspace found; cannot continue");
      filterChain.doFilter(request, response);
      return;
    }

    logger.info("********** querying for workspace id {}", workspaceId);

    // query Sam for the current user's actions on the workspace (resourceActionsV2)
    // this relies on the BearerTokenFilter executing prior to this filter!!
    // BearerTokenFilter extracts the bearer token and saves it in request context;
    // this call to Sam looks for that bearer token.
    SamAuthorizationDao samAuthorizationDao =
        samAuthorizationDaoFactory.getSamAuthorizationDao(workspaceId);
    List<String> userActions = samAuthorizationDao.getWorkspaceActions();

    logger.info("********** found user actions {}", userActions);

    // convert the user's Sam actions for this workspace to a list of GrantedAuthority by
    // prepending "ROLE_" to each action. The "ROLE_" is a convention in Spring Security
    List<SimpleGrantedAuthority> auths =
        userActions.stream()
            .map(samAction -> new SimpleGrantedAuthority("ROLE_" + samAction))
            .toList();

    SamAuthToken auth = new SamAuthToken(auths);

    SecurityContextHolder.getContext().setAuthentication(auth);
    filterChain.doFilter(request, response);
  }

  private WorkspaceId determineWorkspaceId(HttpServletRequest request) {
    String requestPath = request.getRequestURI().toLowerCase();

    logger.info(
        "********** checking request path: {}",
        requestPath);

    // does this request contain a collection id?
    CollectionId collectionId = extractCollectionId(requestPath);
    if (collectionId != null) {
      return collectionService.getWorkspaceId(collectionId);
    }

    // does this request contain a job id?
    UUID jobId = extractJobId(requestPath);
    if (jobId != null) {
      var job = jobService.getJob(jobId);
      return collectionService.getWorkspaceId(CollectionId.of(job.getInstanceId()));
    }
    return null;
  }

  //  private WorkspaceId extractWorkspaceId(String requestUri) {
  //    // TODO: none of the WDS APIs currently include a workspaceId; this will change when the
  //    //  v1 collection APIs are released.
  //    return null;
  //  }

  private static final String UUID_REGEX =
      "([0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12})";

  private static final List<Pattern> COLLECTION_ID_PATTERNS =
      List.of(
          Pattern.compile("^/" + UUID_REGEX + "/types"),
          Pattern.compile("^/" + UUID_REGEX + "/records"),
          Pattern.compile("^/" + UUID_REGEX + "/search"),
          Pattern.compile("^/" + UUID_REGEX + "/tsv"),
          Pattern.compile("^/" + UUID_REGEX + "/batch"),
          Pattern.compile("^/" + UUID_REGEX + "/import"),
          Pattern.compile("^/job/v1/instance/" + UUID_REGEX));

  private CollectionId extractCollectionId(String requestUri) {
    for (Pattern collectionIdPattern : COLLECTION_ID_PATTERNS) {
      Matcher matcher = collectionIdPattern.matcher(requestUri);
      if (matcher.find()) {
        var id = matcher.group(1);
        logger.info(
            "********** found collection id '{}' via pattern '{}'",
            id,
            collectionIdPattern.pattern());
        return CollectionId.fromString(id);
      }
    }
    return null;
  }

  private static final List<Pattern> JOB_ID_PATTERNS =
      List.of(Pattern.compile("^/job/v1/" + UUID_REGEX));

  private UUID extractJobId(String requestUri) {
    for (Pattern jobIdPattern : JOB_ID_PATTERNS) {
      Matcher matcher = jobIdPattern.matcher(requestUri);
      if (matcher.matches()) {
        var id = matcher.group(1);
        logger.info("********** found job id '{}' via pattern '{}'", id, jobIdPattern.pattern());
        return UUID.fromString(id);
      }
    }
    return null;
  }
}
