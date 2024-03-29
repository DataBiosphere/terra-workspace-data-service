import json
from urllib.parse import urljoin
from uuid import uuid4

from ..cwds_smoke_test_case import CwdsSmokeTestCase


class CwdsJobListingTests(CwdsSmokeTestCase):
  @staticmethod
  def job_listing_url(collection_id) -> str:
    """Generate the url to list jobs in a given collection"""
    return urljoin(CwdsSmokeTestCase.build_cwds_url("/job/v1/instance/"), str(collection_id))

  def test_status_code_is_404(self):
    """Call the job-listing url; it should return 404 since we are specifying a random collection"""
    response = CwdsSmokeTestCase.call_cwds(self.job_listing_url(CwdsSmokeTestCase.WORKSPACE_ID),
                                           CwdsSmokeTestCase.USER_TOKEN)
    self.assertEqual(response.status_code, 200,
                     f"Job Listing HTTP Status is not 200: {response.text}")
    job_list = json.loads(response.text)
    self.assertIsInstance(job_list, list, "job listing was not an array")
