# openapi_client.SchemaApi

All URIs are relative to *http://..*

Method | HTTP request | Description
------------- | ------------- | -------------
[**delete_attribute**](SchemaApi.md#delete_attribute) | **DELETE** /{instanceid}/types/{type}/attributes/{v}/{attribute} | Delete attribute (pending)
[**delete_record_type**](SchemaApi.md#delete_record_type) | **DELETE** /{instanceid}/types/{v}/{type} | Delete record type
[**describe_all_record_types**](SchemaApi.md#describe_all_record_types) | **GET** /{instanceid}/types/{v} | Describe all record types
[**describe_attribute**](SchemaApi.md#describe_attribute) | **GET** /{instanceid}/types/{type}/attributes/{v}/{attribute} | Describe attribute (pending)
[**describe_record_type**](SchemaApi.md#describe_record_type) | **GET** /{instanceid}/types/{v}/{type} | Describe record type
[**update_attribute**](SchemaApi.md#update_attribute) | **PATCH** /{instanceid}/types/{type}/attributes/{v}/{attribute} | Update attribute (pending)
[**update_record_type**](SchemaApi.md#update_record_type) | **PATCH** /{instanceid}/types/{v}/{type} | Update record type (pending)


# **delete_attribute**
> delete_attribute(instanceid, v, type, attribute)

Delete attribute (pending)

Delete attribute. This deletes all values for this attribute within the specified type. 

### Example

```python
from __future__ import print_function
import time
import openapi_client
from openapi_client.rest import ApiException
from pprint import pprint
# Defining the host is optional and defaults to http://..
# See configuration.py for a list of all supported configuration parameters.
configuration = openapi_client.Configuration(
    host = "http://.."
)


# Enter a context with an instance of the API client
with openapi_client.ApiClient() as api_client:
    # Create an instance of the API class
    api_instance = openapi_client.SchemaApi(api_client)
    instanceid = 'instanceid_example' # str | WDS instance id; by convention equal to workspace id
v = 'v0.2' # str | API version (default to 'v0.2')
type = 'type_example' # str | Record type
attribute = 'attribute_example' # str | Attribute name

    try:
        # Delete attribute (pending)
        api_instance.delete_attribute(instanceid, v, type, attribute)
    except ApiException as e:
        print("Exception when calling SchemaApi->delete_attribute: %s\n" % e)
```

### Parameters

Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **instanceid** | **str**| WDS instance id; by convention equal to workspace id | 
 **v** | **str**| API version | [default to &#39;v0.2&#39;]
 **type** | **str**| Record type | 
 **attribute** | **str**| Attribute name | 

### Return type

void (empty response body)

### Authorization

No authorization required

### HTTP request headers

 - **Content-Type**: Not defined
 - **Accept**: Not defined

### HTTP response details
| Status code | Description | Response headers |
|-------------|-------------|------------------|
**204** | Success |  -  |

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)

# **delete_record_type**
> delete_record_type(instanceid, v, type)

Delete record type

Delete record type. All records of this type will be deleted.

### Example

```python
from __future__ import print_function
import time
import openapi_client
from openapi_client.rest import ApiException
from pprint import pprint
# Defining the host is optional and defaults to http://..
# See configuration.py for a list of all supported configuration parameters.
configuration = openapi_client.Configuration(
    host = "http://.."
)


# Enter a context with an instance of the API client
with openapi_client.ApiClient() as api_client:
    # Create an instance of the API class
    api_instance = openapi_client.SchemaApi(api_client)
    instanceid = 'instanceid_example' # str | WDS instance id; by convention equal to workspace id
v = 'v0.2' # str | API version (default to 'v0.2')
type = 'type_example' # str | Record type

    try:
        # Delete record type
        api_instance.delete_record_type(instanceid, v, type)
    except ApiException as e:
        print("Exception when calling SchemaApi->delete_record_type: %s\n" % e)
```

### Parameters

Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **instanceid** | **str**| WDS instance id; by convention equal to workspace id | 
 **v** | **str**| API version | [default to &#39;v0.2&#39;]
 **type** | **str**| Record type | 

### Return type

void (empty response body)

### Authorization

No authorization required

### HTTP request headers

 - **Content-Type**: Not defined
 - **Accept**: Not defined

### HTTP response details
| Status code | Description | Response headers |
|-------------|-------------|------------------|
**204** | Success |  -  |
**409** | at least one of the records to be deleted is a relation target |  -  |

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)

# **describe_all_record_types**
> list[RecordTypeSchema] describe_all_record_types(instanceid, v)

Describe all record types

Returns the schema definition for all types in this instance. 

### Example

```python
from __future__ import print_function
import time
import openapi_client
from openapi_client.rest import ApiException
from pprint import pprint
# Defining the host is optional and defaults to http://..
# See configuration.py for a list of all supported configuration parameters.
configuration = openapi_client.Configuration(
    host = "http://.."
)


# Enter a context with an instance of the API client
with openapi_client.ApiClient() as api_client:
    # Create an instance of the API class
    api_instance = openapi_client.SchemaApi(api_client)
    instanceid = 'instanceid_example' # str | WDS instance id; by convention equal to workspace id
v = 'v0.2' # str | API version (default to 'v0.2')

    try:
        # Describe all record types
        api_response = api_instance.describe_all_record_types(instanceid, v)
        pprint(api_response)
    except ApiException as e:
        print("Exception when calling SchemaApi->describe_all_record_types: %s\n" % e)
```

### Parameters

Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **instanceid** | **str**| WDS instance id; by convention equal to workspace id | 
 **v** | **str**| API version | [default to &#39;v0.2&#39;]

### Return type

[**list[RecordTypeSchema]**](RecordTypeSchema.md)

### Authorization

No authorization required

### HTTP request headers

 - **Content-Type**: Not defined
 - **Accept**: application/json

### HTTP response details
| Status code | Description | Response headers |
|-------------|-------------|------------------|
**200** | Success |  -  |
**404** | Instance not found |  -  |

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)

# **describe_attribute**
> AttributeSchema describe_attribute(instanceid, v, type, attribute)

Describe attribute (pending)

Returns the schema definition for this attribute. 

### Example

```python
from __future__ import print_function
import time
import openapi_client
from openapi_client.rest import ApiException
from pprint import pprint
# Defining the host is optional and defaults to http://..
# See configuration.py for a list of all supported configuration parameters.
configuration = openapi_client.Configuration(
    host = "http://.."
)


# Enter a context with an instance of the API client
with openapi_client.ApiClient() as api_client:
    # Create an instance of the API class
    api_instance = openapi_client.SchemaApi(api_client)
    instanceid = 'instanceid_example' # str | WDS instance id; by convention equal to workspace id
v = 'v0.2' # str | API version (default to 'v0.2')
type = 'type_example' # str | Record type
attribute = 'attribute_example' # str | Attribute name

    try:
        # Describe attribute (pending)
        api_response = api_instance.describe_attribute(instanceid, v, type, attribute)
        pprint(api_response)
    except ApiException as e:
        print("Exception when calling SchemaApi->describe_attribute: %s\n" % e)
```

### Parameters

Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **instanceid** | **str**| WDS instance id; by convention equal to workspace id | 
 **v** | **str**| API version | [default to &#39;v0.2&#39;]
 **type** | **str**| Record type | 
 **attribute** | **str**| Attribute name | 

### Return type

[**AttributeSchema**](AttributeSchema.md)

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

# **describe_record_type**
> RecordTypeSchema describe_record_type(instanceid, v, type)

Describe record type

Returns the schema definition for this type. 

### Example

```python
from __future__ import print_function
import time
import openapi_client
from openapi_client.rest import ApiException
from pprint import pprint
# Defining the host is optional and defaults to http://..
# See configuration.py for a list of all supported configuration parameters.
configuration = openapi_client.Configuration(
    host = "http://.."
)


# Enter a context with an instance of the API client
with openapi_client.ApiClient() as api_client:
    # Create an instance of the API class
    api_instance = openapi_client.SchemaApi(api_client)
    instanceid = 'instanceid_example' # str | WDS instance id; by convention equal to workspace id
v = 'v0.2' # str | API version (default to 'v0.2')
type = 'type_example' # str | Record type

    try:
        # Describe record type
        api_response = api_instance.describe_record_type(instanceid, v, type)
        pprint(api_response)
    except ApiException as e:
        print("Exception when calling SchemaApi->describe_record_type: %s\n" % e)
```

### Parameters

Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **instanceid** | **str**| WDS instance id; by convention equal to workspace id | 
 **v** | **str**| API version | [default to &#39;v0.2&#39;]
 **type** | **str**| Record type | 

### Return type

[**RecordTypeSchema**](RecordTypeSchema.md)

### Authorization

No authorization required

### HTTP request headers

 - **Content-Type**: Not defined
 - **Accept**: application/json

### HTTP response details
| Status code | Description | Response headers |
|-------------|-------------|------------------|
**200** | Success |  -  |
**404** | Record type not found |  -  |

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)

# **update_attribute**
> AttributeSchema update_attribute(instanceid, v, type, attribute, attribute_schema=attribute_schema)

Update attribute (pending)

Update attribute. All records of the specified type that contain the old attribute will now have the new attribute instead. When changing datatypes, WDS will make a best effort to cast the existing values into the new datatype. Any values that cannot be successfully cast will be changed to null. Returns the updated attribute definition. 

### Example

```python
from __future__ import print_function
import time
import openapi_client
from openapi_client.rest import ApiException
from pprint import pprint
# Defining the host is optional and defaults to http://..
# See configuration.py for a list of all supported configuration parameters.
configuration = openapi_client.Configuration(
    host = "http://.."
)


# Enter a context with an instance of the API client
with openapi_client.ApiClient() as api_client:
    # Create an instance of the API class
    api_instance = openapi_client.SchemaApi(api_client)
    instanceid = 'instanceid_example' # str | WDS instance id; by convention equal to workspace id
v = 'v0.2' # str | API version (default to 'v0.2')
type = 'type_example' # str | Record type
attribute = 'attribute_example' # str | Attribute name
attribute_schema = openapi_client.AttributeSchema() # AttributeSchema |  (optional)

    try:
        # Update attribute (pending)
        api_response = api_instance.update_attribute(instanceid, v, type, attribute, attribute_schema=attribute_schema)
        pprint(api_response)
    except ApiException as e:
        print("Exception when calling SchemaApi->update_attribute: %s\n" % e)
```

### Parameters

Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **instanceid** | **str**| WDS instance id; by convention equal to workspace id | 
 **v** | **str**| API version | [default to &#39;v0.2&#39;]
 **type** | **str**| Record type | 
 **attribute** | **str**| Attribute name | 
 **attribute_schema** | [**AttributeSchema**](AttributeSchema.md)|  | [optional] 

### Return type

[**AttributeSchema**](AttributeSchema.md)

### Authorization

No authorization required

### HTTP request headers

 - **Content-Type**: application/json
 - **Accept**: application/json

### HTTP response details
| Status code | Description | Response headers |
|-------------|-------------|------------------|
**200** | Success |  -  |
**400** | Malformed payload; or invalid name, datatype, or relation target |  -  |
**409** | Target attribute name already exists, for a rename operation |  -  |

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)

# **update_record_type**
> RecordTypeSchema update_record_type(instanceid, v, type, record_type_schema=record_type_schema)

Update record type (pending)

Update record type. All records of the old type will be updated to the new type. This API can be used to rename a record type and/or to perform batch updates to this type's attributes; see also the updateAttribute API. This API cannot be used to delete attributes; use deleteAttribute instead. Returns the updated type definition. 

### Example

```python
from __future__ import print_function
import time
import openapi_client
from openapi_client.rest import ApiException
from pprint import pprint
# Defining the host is optional and defaults to http://..
# See configuration.py for a list of all supported configuration parameters.
configuration = openapi_client.Configuration(
    host = "http://.."
)


# Enter a context with an instance of the API client
with openapi_client.ApiClient() as api_client:
    # Create an instance of the API class
    api_instance = openapi_client.SchemaApi(api_client)
    instanceid = 'instanceid_example' # str | WDS instance id; by convention equal to workspace id
v = 'v0.2' # str | API version (default to 'v0.2')
type = 'type_example' # str | Record type
record_type_schema = openapi_client.RecordTypeSchema() # RecordTypeSchema |  (optional)

    try:
        # Update record type (pending)
        api_response = api_instance.update_record_type(instanceid, v, type, record_type_schema=record_type_schema)
        pprint(api_response)
    except ApiException as e:
        print("Exception when calling SchemaApi->update_record_type: %s\n" % e)
```

### Parameters

Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **instanceid** | **str**| WDS instance id; by convention equal to workspace id | 
 **v** | **str**| API version | [default to &#39;v0.2&#39;]
 **type** | **str**| Record type | 
 **record_type_schema** | [**RecordTypeSchema**](RecordTypeSchema.md)|  | [optional] 

### Return type

[**RecordTypeSchema**](RecordTypeSchema.md)

### Authorization

No authorization required

### HTTP request headers

 - **Content-Type**: application/json
 - **Accept**: application/json

### HTTP response details
| Status code | Description | Response headers |
|-------------|-------------|------------------|
**200** | Success |  -  |
**400** | Malformed payload; or invalid type name; or invalid attribute name, datatype, or relation target |  -  |
**409** | Target type or attribute name already exists, for a rename operation |  -  |

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)

