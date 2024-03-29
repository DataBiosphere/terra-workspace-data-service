"""
cWDS Smoke Test
Enter the host (domain and optional port) of the cWDS instance you want to test.
This test will ensure that the cWDS instance running on that host is minimally functional.
"""

import argparse
import requests
import sys
import unittest
from unittest import TestSuite

from tests.authenticated.cwds_job_listing_tests import CwdsJobListingTests
from tests.authenticated.cwds_job_status_tests import CwdsJobStatusTests
from tests.cwds_smoke_test_case import CwdsSmokeTestCase
from tests.unauthenticated.cwds_status_tests import CwdsStatusTests
from tests.unauthenticated.cwds_version_tests import CwdsVersionTests


def gather_tests(is_authenticated: bool = False) -> TestSuite:
  suite = unittest.TestSuite()

  status_tests = unittest.defaultTestLoader.loadTestsFromTestCase(CwdsStatusTests)
  version_tests = unittest.defaultTestLoader.loadTestsFromTestCase(CwdsVersionTests)

  suite.addTests(status_tests)
  suite.addTests(version_tests)

  if is_authenticated:
    print("user_token and workspace_id both provided.  Running additional authenticated tests.")
    user_info_tests = unittest.defaultTestLoader.loadTestsFromTestCase(CwdsJobListingTests)
    resource_types_tests = unittest.defaultTestLoader.loadTestsFromTestCase(CwdsJobStatusTests)

    suite.addTests(user_info_tests)
    suite.addTests(resource_types_tests)
  else:
    print("user_token and/or workspace_id not provided.  Skipping authenticated tests.")

  return suite


def main(main_args):
  if main_args.user_token:
    verify_user_token(main_args.user_token)

  CwdsSmokeTestCase.CWDS_HOST = main_args.CWDS_HOST
  CwdsSmokeTestCase.WORKSPACE_ID = main_args.workspace_id
  CwdsSmokeTestCase.USER_TOKEN = main_args.user_token

  test_suite = gather_tests(main_args.user_token and main_args.workspace_id)

  runner = unittest.TextTestRunner(verbosity=main_args.verbosity)
  result = runner.run(test_suite)

  # system exit if any tests fail
  if result.failures or result.errors:
    sys.exit(1)


def verify_user_token(user_token: str) -> bool:
  response = requests.get(
    f"https://www.googleapis.com/oauth2/v1/tokeninfo?access_token={user_token}")
  assert response.status_code == 200, "User Token is no longer valid.  Please generate a new token and try again."


if __name__ == "__main__":
  parser = argparse.ArgumentParser(
    description=__doc__,
    formatter_class=argparse.RawTextHelpFormatter
  )
  parser.add_argument(
    "-v",
    "--verbosity",
    type=int,
    choices=[0, 1, 2],
    default=1,
    help="""Python unittest verbosity setting:
  0: Quiet - Prints only number of tests executed
  1: Minimal - (default) Prints number of tests executed plus a dot for each success and an F for each failure
  2: Verbose - Help string and its result will be printed for each test"""
  )
  parser.add_argument(
    "CWDS_HOST",
    help="domain with optional port number of the cWDS host you want to test"
  )
  parser.add_argument(
    "workspace_id",
    nargs='?',
    default=None,
    help="Optional; workspace id against which tests run. If this and user_token are present, enables additional tests."
  )
  parser.add_argument(
    "user_token",
    nargs='?',
    default=None,
    help="Optional; auth token for authenticated tests. If this and workspace_id are present, enables additional tests."
  )

  args = parser.parse_args()

  main(args)
  sys.exit(0)
