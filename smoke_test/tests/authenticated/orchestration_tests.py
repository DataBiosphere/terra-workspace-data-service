import re
import requests
import json

from uuid import uuid4
from functools import cache
from requests import Response
from unittest import TestCase
from urllib.parse import urljoin


class OrchestrationTests(TestCase):
  ORCHESTRATION_HOST = None
  WORKSPACE_NAMESPACE = None
  WORKSPACE_NAME = None
  USER_TOKEN = None

  @staticmethod
  def build_orchestration_url(path: str) -> str:
    assert OrchestrationTests.ORCHESTRATION_HOST, "ERROR - OrchestrationTests.ORCHESTRATION_HOST not properly set"
    if re.match(r"^\s*https?://", OrchestrationTests.ORCHESTRATION_HOST):
      return urljoin(OrchestrationTests.ORCHESTRATION_HOST, path)
    else:
      return urljoin(f"https://{OrchestrationTests.ORCHESTRATION_HOST}", path)

  @staticmethod
  @cache
  def call_orchestration(path: str) -> Response:
    """Function is memoized so that we only make the call once"""
    headers = { "Authorization": f"Bearer {OrchestrationTests.USER_TOKEN}" }
    return requests.get(OrchestrationTests.build_orchestration_url(path), headers=headers)

  @staticmethod
  def workspace_path() -> str:
    """Generate the path for the configured workspace"""
    return f"/api/workspaces/{OrchestrationTests.WORKSPACE_NAMESPACE}/{OrchestrationTests.WORKSPACE_NAME}"

  @staticmethod
  def job_path(job_id) -> str:
    """Generate the url to get status of a given job id"""
    return f"{OrchestrationTests.workspace_path()}/importJob/{job_id}"

  @staticmethod
  def job_listing_path() -> str:
    """Generate the path to list jobs in a given workspace"""
    return f"{OrchestrationTests.workspace_path()}/importJob?runningOnly=true"

  def test_random_job_status_is_404(self):
    """Call the job-status url; it should return 404 since we are specifying a random job id"""
    response = OrchestrationTests.call_orchestration(self.job_path(uuid4()))
    self.assertEqual(response.status_code, 404,
                     f"Job Status HTTP Status is not 404: {response.text}")

  def test_job_listing_status_is_200_with_array(self):
    """Call the job-listing url; it should return 200 and the response should be an array"""
    response = OrchestrationTests.call_orchestration(self.job_listing_path())
    self.assertEqual(response.status_code, 200,
                     f"Job Listing HTTP Status is not 200: {response.text}")
    job_list = json.loads(response.text)
    self.assertIsInstance(job_list, list, "job listing was not an array")
