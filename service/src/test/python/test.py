# test.py

from unittest import TestCase
import  wds_client

class TryTesting(TestCase):
    def check_version(self):
        api_client = wds_client.ApiClient()
        api_client.configuration.host = 'http://localhost:8080'
        generalInfo_instance = wds_client.GeneralWDSInformationApi(api_client)
        response = generalInfo_instance.version_get()
        print(response.build.version)
        self.assertIsNotNone(response.build.version)
 
    def check_status(self):
        api_client = wds_client.ApiClient()
        api_client.configuration.host = 'http://localhost:8080'
        generalInfo_instance = wds_client.GeneralWDSInformationApi(api_client)
        response = generalInfo_instance.status_get()
        print(response.status)
        self.assertEqual(response.status, "UP")
