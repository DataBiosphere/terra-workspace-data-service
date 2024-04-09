import re
import requests
from functools import cache
from requests import Response
from unittest import TestCase
from urllib.parse import urljoin


class CwdsSmokeTestCase(TestCase):
  CWDS_HOST = None
  WORKSPACE_ID = None
  USER_TOKEN = None

  @staticmethod
  def build_cwds_url(path: str) -> str:
    assert CwdsSmokeTestCase.CWDS_HOST, "ERROR - CwdsSmokeTestCase.CWDS_HOST not properly set"
    if re.match(r"^\s*https?://", CwdsSmokeTestCase.CWDS_HOST):
      return urljoin(CwdsSmokeTestCase.CWDS_HOST, path)
    else:
      return urljoin(f"https://{CwdsSmokeTestCase.CWDS_HOST}", path)

  @staticmethod
  @cache
  def call_cwds(url: str, user_token: str = None) -> Response:
    """Function is memoized so that we only make the call once"""
    headers = {"Authorization": f"Bearer {user_token}"} if user_token else {}
    return requests.get(url, headers=headers)
