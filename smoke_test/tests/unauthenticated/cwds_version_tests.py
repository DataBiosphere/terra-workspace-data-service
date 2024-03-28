import json

from ..cwds_smoke_test_case import CwdsSmokeTestCase


class CwdsVersionTests(CwdsSmokeTestCase):
  @staticmethod
  def version_url() -> str:
    """Generate the url to /version"""
    return CwdsSmokeTestCase.build_cwds_url("/version")

  def test_status_code_is_200(self):
    """Call /version; it should return 200"""
    response = CwdsSmokeTestCase.call_cwds(self.version_url())
    self.assertEqual(response.status_code, 200)

  def test_version_value_specified(self):
    """Call /version; it should contain a non-empty build.version value"""
    response = CwdsSmokeTestCase.call_cwds(self.version_url())
    version = json.loads(response.text)
    self.assertIsNotNone(version["build"]["version"], "Version value must be non-empty")
