# wds_client.GeneralWDSInformationApi

All URIs are relative to *http://..*

Method | HTTP request | Description
------------- | ------------- | -------------
[**status_get**](GeneralWDSInformationApi.md#status_get) | **GET** /status | Gets health status for WDS -- generated via Spring Boot Actuator (see https://docs.spring.io/spring-boot/docs/current/actuator-api/htmlsingle/#health for details)
[**version_get**](GeneralWDSInformationApi.md#version_get) | **GET** /version | Gets related git and build version info for WDS -- generated via Spring Boot Actuator (see https://docs.spring.io/spring-boot/docs/current/actuator-api/htmlsingle/#info for details)


# **status_get**
> StatusResponse status_get()

Gets health status for WDS -- generated via Spring Boot Actuator (see https://docs.spring.io/spring-boot/docs/current/actuator-api/htmlsingle/#health for details)

### Example

```python
from __future__ import print_function
import time
import wds_client
from wds_client.rest import ApiException
from pprint import pprint
# Defining the host is optional and defaults to http://..
# See configuration.py for a list of all supported configuration parameters.
configuration = wds_client.Configuration(
    host = "http://.."
)


# Enter a context with an instance of the API client
with wds_client.ApiClient() as api_client:
    # Create an instance of the API class
    api_instance = wds_client.GeneralWDSInformationApi(api_client)
    
    try:
        # Gets health status for WDS -- generated via Spring Boot Actuator (see https://docs.spring.io/spring-boot/docs/current/actuator-api/htmlsingle/#health for details)
        api_response = api_instance.status_get()
        pprint(api_response)
    except ApiException as e:
        print("Exception when calling GeneralWDSInformationApi->status_get: %s\n" % e)
```

### Parameters
This endpoint does not need any parameter.

### Return type

[**StatusResponse**](StatusResponse.md)

### Authorization

No authorization required

### HTTP request headers

 - **Content-Type**: Not defined
 - **Accept**: application/json

### HTTP response details
| Status code | Description | Response headers |
|-------------|-------------|------------------|
**200** | Status Info |  -  |

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)

# **version_get**
> VersionResponse version_get()

Gets related git and build version info for WDS -- generated via Spring Boot Actuator (see https://docs.spring.io/spring-boot/docs/current/actuator-api/htmlsingle/#info for details)

### Example

```python
from __future__ import print_function
import time
import wds_client
from wds_client.rest import ApiException
from pprint import pprint
# Defining the host is optional and defaults to http://..
# See configuration.py for a list of all supported configuration parameters.
configuration = wds_client.Configuration(
    host = "http://.."
)


# Enter a context with an instance of the API client
with wds_client.ApiClient() as api_client:
    # Create an instance of the API class
    api_instance = wds_client.GeneralWDSInformationApi(api_client)
    
    try:
        # Gets related git and build version info for WDS -- generated via Spring Boot Actuator (see https://docs.spring.io/spring-boot/docs/current/actuator-api/htmlsingle/#info for details)
        api_response = api_instance.version_get()
        pprint(api_response)
    except ApiException as e:
        print("Exception when calling GeneralWDSInformationApi->version_get: %s\n" % e)
```

### Parameters
This endpoint does not need any parameter.

### Return type

[**VersionResponse**](VersionResponse.md)

### Authorization

No authorization required

### HTTP request headers

 - **Content-Type**: Not defined
 - **Accept**: application/json

### HTTP response details
| Status code | Description | Response headers |
|-------------|-------------|------------------|
**200** | Version Info |  -  |

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)

