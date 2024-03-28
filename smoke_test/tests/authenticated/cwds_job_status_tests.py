from urllib.parse import urljoin
from uuid import uuid4

from ..cwds_smoke_test_case import CwdsSmokeTestCase


class CwdsJobStatusTests(CwdsSmokeTestCase):
  @staticmethod
  def job_status_url(job_id) -> str:
    """Generate the url to get status of a given job id"""
    return urljoin(CwdsSmokeTestCase.build_cwds_url("/job/v1/"), str(job_id))

  def test_status_code_is_404(self):
    """Call the job-status url; it should return 404 since we are specifying a random job id"""
    response = CwdsSmokeTestCase.call_cwds(self.job_status_url(uuid4()),
                                           CwdsSmokeTestCase.USER_TOKEN)
    self.assertEqual(response.status_code, 404,
                     f"Job Status HTTP Status is not 404: {response.text}")
