# wds_client.InstancesApi

All URIs are relative to *http://..*

Method | HTTP request | Description
------------- | ------------- | -------------
[**create_wds_instance**](InstancesApi.md#create_wds_instance) | **POST** /instances/{v}/{instanceid} | Create an instance
[**delete_wds_instance**](InstancesApi.md#delete_wds_instance) | **DELETE** /instances/{v}/{instanceid} | Delete an instance
[**list_wds_instances**](InstancesApi.md#list_wds_instances) | **GET** /instances/{v} | List instances


# **create_wds_instance**
> create_wds_instance(instanceid, v)

Create an instance

Create an instance with the given UUID. This API is liable to change.

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
    api_instance = wds_client.InstancesApi(api_client)
    instanceid = 'instanceid_example' # str | WDS instance id; by convention equal to workspace id
v = 'v0.2' # str | API version (default to 'v0.2')

    try:
        # Create an instance
        api_instance.create_wds_instance(instanceid, v)
    except ApiException as e:
        print("Exception when calling InstancesApi->create_wds_instance: %s\n" % e)
```

### Parameters

Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **instanceid** | **str**| WDS instance id; by convention equal to workspace id | 
 **v** | **str**| API version | [default to &#39;v0.2&#39;]

### Return type

void (empty response body)

### Authorization

No authorization required

### HTTP request headers

 - **Content-Type**: Not defined
 - **Accept**: application/json

### HTTP response details
| Status code | Description | Response headers |
|-------------|-------------|------------------|
**201** | Success |  -  |
**409** | Conflict - schema already exists. |  -  |

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)

# **delete_wds_instance**
> delete_wds_instance(instanceid, v)

Delete an instance

Delete the instance with the given UUID. This API is liable to change.  THIS WILL DELETE ALL DATA WITHIN THE INSTANCE. Be certain this is what you want to do. 

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
    api_instance = wds_client.InstancesApi(api_client)
    instanceid = 'instanceid_example' # str | WDS instance id; by convention equal to workspace id
v = 'v0.2' # str | API version (default to 'v0.2')

    try:
        # Delete an instance
        api_instance.delete_wds_instance(instanceid, v)
    except ApiException as e:
        print("Exception when calling InstancesApi->delete_wds_instance: %s\n" % e)
```

### Parameters

Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **instanceid** | **str**| WDS instance id; by convention equal to workspace id | 
 **v** | **str**| API version | [default to &#39;v0.2&#39;]

### Return type

void (empty response body)

### Authorization

No authorization required

### HTTP request headers

 - **Content-Type**: Not defined
 - **Accept**: application/json

### HTTP response details
| Status code | Description | Response headers |
|-------------|-------------|------------------|
**200** | Success |  -  |
**404** | Not Found - instance does not exist. |  -  |

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)

# **list_wds_instances**
> list[str] list_wds_instances(v)

List instances

List all instances in this server.

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
    api_instance = wds_client.InstancesApi(api_client)
    v = 'v0.2' # str | API version (default to 'v0.2')

    try:
        # List instances
        api_response = api_instance.list_wds_instances(v)
        pprint(api_response)
    except ApiException as e:
        print("Exception when calling InstancesApi->list_wds_instances: %s\n" % e)
```

### Parameters

Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **v** | **str**| API version | [default to &#39;v0.2&#39;]

### Return type

**list[str]**

### Authorization

No authorization required

### HTTP request headers

 - **Content-Type**: Not defined
 - **Accept**: application/json

### HTTP response details
| Status code | Description | Response headers |
|-------------|-------------|------------------|
**200** | Success |  -  |

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)

