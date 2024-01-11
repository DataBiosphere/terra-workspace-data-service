# Running python tests locally

This page provides instruction around how to replicate python tests failures that happen in the [following](https://github.com/DataBiosphere/terra-workspace-data-service/actions/workflows/release-python-client.yml) github action locally. You can do this on your dev machine or set this up inside a virtual environment. 

## Set up

First thing is first, dont forget to have [WDS running locally](../README.md#setup). You will need a WDS instance locally to ensure python tests have a WDS instance to connect to.

You will need a local python installation - doesnt really matter which version, since all should work but WDS defaults to python3.8. To check if you have python already installed, run the following: 
```
python --version
```

You will also need to have openapi-generator-cli installed as well. 
```
npm install @openapitools/openapi-generator-cli -g
```

Set openapi-generator-cli to the correct version (same as what the github action is using). Depending on your user permissions, sudo may be required to run these commands. 
```
openapi-generator-cli version-manager set 4.3.1
```

At times, openapi-generator-cli wont set to right version even if the command above is run, so it is recommended to verify. Sometimes the proper version needs to be explicitly downloaded before it can be set. To see all available commands: 
```
openapi-generator-cli
```

Run this command to see what versions are downloaded, if 4.3.1 is not, use arrow keys to navigate to it and download it (or if already downloaded to use it). 
```
openapi-generator-cli version-manager list
```

To run tests, WDS uses pytest. Ensure that is installed by running: 
```
pip install pytest
```

## Build wds_client locally

Once you confirm that you have python and openapitools, you will need to build and create a local version of the wds_client package (make sure you are in the right branch that has the changes you want to test). Note that you will need to re-generate the client for each code change you make. To do that, run the following command (from the root of your repo or adjust path accordingly). If this command is generating errors, it is likely because the openapi is set to the wrong version. Note that package version is hard coded since it is not important to track locally - adjust as you se fit if you want to the version to change when you install the package locally. 
```
  openapi-generator-cli generate \
  -i service/src/main/resources/static/swagger/openapi-docs.yaml \
  -g python \
  -o wds-client \
  --additional-properties=projectName=wds-client,packageName=wds_client,packageVersion=0.0.1 \
  --skip-validate-spec
```

This will generate a folder called wds-client, go ahead and "cd" into that folder. Next, run this command to install wds_client locally:
```
pip install .
```

That should use your local python installation to install the wds_client locally. Once complete with no errors, you should be able to open a python shell and run "import wds_client" with no error. However, if you are still not able to do that, it is possible that your pip is tied to a different version of python installed on your machine that is not the default. There are a few ways to solve this problem but the easiest way to go about this is to specify a python version to run pip, something like this:
```
python3 -m pip install .
```

## Running tests

Once you have the wds_client package importing locally with no issues, you are ready to run the tests! Run the following command from the repo root (or adjust path accordingly): 
```
pytest service/src/test/python/test.py
```

Note: Make sure your local WDS database does not have data in it before running these tests, as its presence may cause the tests to fail.

To debug tests, you may want to try out python's [pdb debugger](https://realpython.com/python-debugging-pdb/).

In case you get frustrated with python, go [here](https://xkcd.com/1987/) for a quick laugh. 
