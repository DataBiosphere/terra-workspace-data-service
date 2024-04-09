import re
import requests
from functools import cache
from requests import Response
from unittest import TestCase
from urllib.parse import urljoin


class OrchestrationSmokeTestCase(TestCase):
  ORCHESTRATION_HOST = None
  WORKSPACE_NAMESPACE = None
  WORKSPACE_NAME = None
  USER_TOKEN = None

  @staticmethod
  def build_orchestration_url(path: str) -> str:
    assert OrchestrationSmokeTestCase.ORCHESTRATION_HOST, "ERROR - OrchestrationSmokeTestCase.ORCHESTRATION_HOST not properly set"
    if re.match(r"^\s*https?://", OrchestrationSmokeTestCase.ORCHESTRATION_HOST):
      return urljoin(OrchestrationSmokeTestCase.ORCHESTRATION_HOST, path)
    else:
      return urljoin(f"https://{OrchestrationSmokeTestCase.ORCHESTRATION_HOST}", path)

  @staticmethod
  @cache
  def call_orchestration(path: str) -> Response:
    """Function is memoized so that we only make the call once"""
    user_token = OrchestrationSmokeTestCase.USER_TOKEN
    headers = {"Authorization": f"Bearer {user_token}"} if user_token else {}
    return requests.get(OrchestrationSmokeTestCase.build_orchestration_url(path), headers=headers)
