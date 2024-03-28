import json

from ..cwds_smoke_test_case import CwdsSmokeTestCase


class CwdsStatusTests(CwdsSmokeTestCase):
  @staticmethod
  def status_url() -> str:
    """Generate the url to /status"""
    return CwdsSmokeTestCase.build_cwds_url("/status")

  def test_status_code_is_200(self):
    """Call /status; it should return 200 with status=UP"""
    response = CwdsSmokeTestCase.call_cwds(self.status_url())
    self.assertEqual(response.status_code, 200)
    status = json.loads(response.text)
    self.assertEqual(status["status"], "UP", "status is not UP")

  def test_subsystems(self):
    """Call /status; each component should have status=UP"""
    response = CwdsSmokeTestCase.call_cwds(self.status_url())
    status = json.loads(response.text)
    for component in status["components"]:
      self.assertEqual(status["components"][component]["status"], "UP", f"{component} is not OK")
