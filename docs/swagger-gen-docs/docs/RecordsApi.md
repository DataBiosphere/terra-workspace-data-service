# wds_client.RecordsApi

All URIs are relative to *http://..*

Method | HTTP request | Description
------------- | ------------- | -------------
[**batch_write_records**](RecordsApi.md#batch_write_records) | **POST** /{instanceid}/batch/{v}/{type} | Batch write records
[**create_or_replace_record**](RecordsApi.md#create_or_replace_record) | **PUT** /{instanceid}/records/{v}/{type}/{id} | Create or replace record
[**delete_record**](RecordsApi.md#delete_record) | **DELETE** /{instanceid}/records/{v}/{type}/{id} | Delete record
[**get_record**](RecordsApi.md#get_record) | **GET** /{instanceid}/records/{v}/{type}/{id} | Get record
[**get_records_as_tsv**](RecordsApi.md#get_records_as_tsv) | **GET** /{instanceid}/tsv/{v}/{type} | Retrieve all records in record type as tsv.
[**query_records**](RecordsApi.md#query_records) | **POST** /{instanceid}/search/{v}/{type} | Query records
[**update_record**](RecordsApi.md#update_record) | **PATCH** /{instanceid}/records/{v}/{type}/{id} | Update record
[**upload_tsv**](RecordsApi.md#upload_tsv) | **POST** /{instanceid}/tsv/{v}/{type} | Import records to a record type from a tsv file


# **batch_write_records**
> BatchResponse batch_write_records(instanceid, v, type, batch_operation, primary_key=primary_key)

Batch write records

Perform a batch of upsert / delete operations on multiple records

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
    api_instance = wds_client.RecordsApi(api_client)
    instanceid = 'instanceid_example' # str | WDS instance id; by convention equal to workspace id
v = 'v0.2' # str | API version (default to 'v0.2')
type = 'type_example' # str | Record type
batch_operation = [wds_client.BatchOperation()] # list[BatchOperation] | A list of batch operations to perform on records
primary_key = 'primary_key_example' # str | the column to uniquely identify a record (optional)

    try:
        # Batch write records
        api_response = api_instance.batch_write_records(instanceid, v, type, batch_operation, primary_key=primary_key)
        pprint(api_response)
    except ApiException as e:
        print("Exception when calling RecordsApi->batch_write_records: %s\n" % e)
```

### Parameters

Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **instanceid** | **str**| WDS instance id; by convention equal to workspace id | 
 **v** | **str**| API version | [default to &#39;v0.2&#39;]
 **type** | **str**| Record type | 
 **batch_operation** | [**list[BatchOperation]**](BatchOperation.md)| A list of batch operations to perform on records | 
 **primary_key** | **str**| the column to uniquely identify a record | [optional] 

### Return type

[**BatchResponse**](BatchResponse.md)

### Authorization

No authorization required

### HTTP request headers

 - **Content-Type**: application/json
 - **Accept**: application/json

### HTTP response details
| Status code | Description | Response headers |
|-------------|-------------|------------------|
**200** | Success |  -  |

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)

# **create_or_replace_record**
> RecordResponse create_or_replace_record(instanceid, v, type, id, record_request, primary_key=primary_key)

Create or replace record

Creates or replaces the record using the specified type and id. If the record already exists, its entire set of attributes will be overwritten by the attributes in the request body. TODO: add a query parameter to allow/disallow overwriting existing records? 

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
    api_instance = wds_client.RecordsApi(api_client)
    instanceid = 'instanceid_example' # str | WDS instance id; by convention equal to workspace id
v = 'v0.2' # str | API version (default to 'v0.2')
type = 'type_example' # str | Record type
id = 'id_example' # str | Record id
record_request = wds_client.RecordRequest() # RecordRequest | A record's attributes to upload
primary_key = 'primary_key_example' # str | the column to uniquely identify a record (optional)

    try:
        # Create or replace record
        api_response = api_instance.create_or_replace_record(instanceid, v, type, id, record_request, primary_key=primary_key)
        pprint(api_response)
    except ApiException as e:
        print("Exception when calling RecordsApi->create_or_replace_record: %s\n" % e)
```

### Parameters

Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **instanceid** | **str**| WDS instance id; by convention equal to workspace id | 
 **v** | **str**| API version | [default to &#39;v0.2&#39;]
 **type** | **str**| Record type | 
 **id** | **str**| Record id | 
 **record_request** | [**RecordRequest**](RecordRequest.md)| A record&#39;s attributes to upload | 
 **primary_key** | **str**| the column to uniquely identify a record | [optional] 

### Return type

[**RecordResponse**](RecordResponse.md)

### Authorization

No authorization required

### HTTP request headers

 - **Content-Type**: application/json
 - **Accept**: application/json

### HTTP response details
| Status code | Description | Response headers |
|-------------|-------------|------------------|
**200** | A record |  -  |
**201** | A record |  -  |
**400** | Bad request |  -  |

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)

# **delete_record**
> delete_record(instanceid, v, type, id)

Delete record

Deletes the record at the specified type and id.

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
    api_instance = wds_client.RecordsApi(api_client)
    instanceid = 'instanceid_example' # str | WDS instance id; by convention equal to workspace id
v = 'v0.2' # str | API version (default to 'v0.2')
type = 'type_example' # str | Record type
id = 'id_example' # str | Record id

    try:
        # Delete record
        api_instance.delete_record(instanceid, v, type, id)
    except ApiException as e:
        print("Exception when calling RecordsApi->delete_record: %s\n" % e)
```

### Parameters

Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **instanceid** | **str**| WDS instance id; by convention equal to workspace id | 
 **v** | **str**| API version | [default to &#39;v0.2&#39;]
 **type** | **str**| Record type | 
 **id** | **str**| Record id | 

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
**204** | Success |  -  |
**404** | Record not found |  -  |
**400** | Bad request |  -  |

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)

# **get_record**
> RecordResponse get_record(instanceid, v, type, id)

Get record

Retrieves a single record by its type and id

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
    api_instance = wds_client.RecordsApi(api_client)
    instanceid = 'instanceid_example' # str | WDS instance id; by convention equal to workspace id
v = 'v0.2' # str | API version (default to 'v0.2')
type = 'type_example' # str | Record type
id = 'id_example' # str | Record id

    try:
        # Get record
        api_response = api_instance.get_record(instanceid, v, type, id)
        pprint(api_response)
    except ApiException as e:
        print("Exception when calling RecordsApi->get_record: %s\n" % e)
```

### Parameters

Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **instanceid** | **str**| WDS instance id; by convention equal to workspace id | 
 **v** | **str**| API version | [default to &#39;v0.2&#39;]
 **type** | **str**| Record type | 
 **id** | **str**| Record id | 

### Return type

[**RecordResponse**](RecordResponse.md)

### Authorization

No authorization required

### HTTP request headers

 - **Content-Type**: Not defined
 - **Accept**: application/json

### HTTP response details
| Status code | Description | Response headers |
|-------------|-------------|------------------|
**200** | A record |  -  |
**404** | Record not found |  -  |

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)

# **get_records_as_tsv**
> file get_records_as_tsv(instanceid, v, type)

Retrieve all records in record type as tsv.

Streams all records in a record type to a tsv format.

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
    api_instance = wds_client.RecordsApi(api_client)
    instanceid = 'instanceid_example' # str | WDS instance id; by convention equal to workspace id
v = 'v0.2' # str | API version (default to 'v0.2')
type = 'type_example' # str | Record type

    try:
        # Retrieve all records in record type as tsv.
        api_response = api_instance.get_records_as_tsv(instanceid, v, type)
        pprint(api_response)
    except ApiException as e:
        print("Exception when calling RecordsApi->get_records_as_tsv: %s\n" % e)
```

### Parameters

Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **instanceid** | **str**| WDS instance id; by convention equal to workspace id | 
 **v** | **str**| API version | [default to &#39;v0.2&#39;]
 **type** | **str**| Record type | 

### Return type

**file**

### Authorization

No authorization required

### HTTP request headers

 - **Content-Type**: Not defined
 - **Accept**: text/tab-separated-values, application/json

### HTTP response details
| Status code | Description | Response headers |
|-------------|-------------|------------------|
**200** | Records in tsv format |  -  |
**404** | Instance or Record type not found |  -  |

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)

# **query_records**
> RecordQueryResponse query_records(instanceid, v, type, search_request)

Query records

Paginated list of records matching the criteria supplied in the request body

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
    api_instance = wds_client.RecordsApi(api_client)
    instanceid = 'instanceid_example' # str | WDS instance id; by convention equal to workspace id
v = 'v0.2' # str | API version (default to 'v0.2')
type = 'type_example' # str | Record type
search_request = wds_client.SearchRequest() # SearchRequest | A paginated search request

    try:
        # Query records
        api_response = api_instance.query_records(instanceid, v, type, search_request)
        pprint(api_response)
    except ApiException as e:
        print("Exception when calling RecordsApi->query_records: %s\n" % e)
```

### Parameters

Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **instanceid** | **str**| WDS instance id; by convention equal to workspace id | 
 **v** | **str**| API version | [default to &#39;v0.2&#39;]
 **type** | **str**| Record type | 
 **search_request** | [**SearchRequest**](SearchRequest.md)| A paginated search request | 

### Return type

[**RecordQueryResponse**](RecordQueryResponse.md)

### Authorization

No authorization required

### HTTP request headers

 - **Content-Type**: application/json
 - **Accept**: application/json

### HTTP response details
| Status code | Description | Response headers |
|-------------|-------------|------------------|
**200** | Success |  -  |
**404** | Record type not found |  -  |

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)

# **update_record**
> RecordResponse update_record(instanceid, v, type, id, record_request)

Update record

Updates the record of the specified type and id. Any attributes included in the request body will be created or overwritten. Attributes not included in the request body will be untouched in the database. No attributes will be deleted. To delete attributes, use the PUT api instead. 

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
    api_instance = wds_client.RecordsApi(api_client)
    instanceid = 'instanceid_example' # str | WDS instance id; by convention equal to workspace id
v = 'v0.2' # str | API version (default to 'v0.2')
type = 'type_example' # str | Record type
id = 'id_example' # str | Record id
record_request = wds_client.RecordRequest() # RecordRequest | A record's attributes to upload

    try:
        # Update record
        api_response = api_instance.update_record(instanceid, v, type, id, record_request)
        pprint(api_response)
    except ApiException as e:
        print("Exception when calling RecordsApi->update_record: %s\n" % e)
```

### Parameters

Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **instanceid** | **str**| WDS instance id; by convention equal to workspace id | 
 **v** | **str**| API version | [default to &#39;v0.2&#39;]
 **type** | **str**| Record type | 
 **id** | **str**| Record id | 
 **record_request** | [**RecordRequest**](RecordRequest.md)| A record&#39;s attributes to upload | 

### Return type

[**RecordResponse**](RecordResponse.md)

### Authorization

No authorization required

### HTTP request headers

 - **Content-Type**: application/json
 - **Accept**: application/json

### HTTP response details
| Status code | Description | Response headers |
|-------------|-------------|------------------|
**200** | A record |  -  |
**404** | Record not found |  -  |
**400** | Bad request |  -  |

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)

# **upload_tsv**
> TsvUploadResponse upload_tsv(instanceid, v, type, records, primary_key=primary_key)

Import records to a record type from a tsv file

Upload a tsv to modify or create records in a record type.  This operation will insert or update records.

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
    api_instance = wds_client.RecordsApi(api_client)
    instanceid = 'instanceid_example' # str | WDS instance id; by convention equal to workspace id
v = 'v0.2' # str | API version (default to 'v0.2')
type = 'type_example' # str | Record type
records = '/path/to/file' # file | A valid TSV import file
primary_key = 'primary_key_example' # str | the column to uniquely identify a record (optional)

    try:
        # Import records to a record type from a tsv file
        api_response = api_instance.upload_tsv(instanceid, v, type, records, primary_key=primary_key)
        pprint(api_response)
    except ApiException as e:
        print("Exception when calling RecordsApi->upload_tsv: %s\n" % e)
```

### Parameters

Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **instanceid** | **str**| WDS instance id; by convention equal to workspace id | 
 **v** | **str**| API version | [default to &#39;v0.2&#39;]
 **type** | **str**| Record type | 
 **records** | **file**| A valid TSV import file | 
 **primary_key** | **str**| the column to uniquely identify a record | [optional] 

### Return type

[**TsvUploadResponse**](TsvUploadResponse.md)

### Authorization

No authorization required

### HTTP request headers

 - **Content-Type**: multipart/form-data
 - **Accept**: application/json

### HTTP response details
| Status code | Description | Response headers |
|-------------|-------------|------------------|
**200** | Success |  -  |
**400** | Bad Request |  -  |
**404** | Instance not found |  -  |

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)

