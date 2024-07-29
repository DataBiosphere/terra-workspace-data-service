#!/bin/bash

set -e

MAX_RETRIES=${MAX_RETRIES:-15}
SLEEP_TIME=${SLEEP_TIME:-5}

# Mask the GitHub token to prevent it from being logged
function mask_token {
  echo "::add-mask::$GITHUB_TOKEN"
}

function get_app_context_from_artifact {
  local sha=$1
  local short_sha=${sha:0:7}
  local retry_count=0
  local sleep_time=$((${SLEEP_TIME}))
  local max_retries=$((${MAX_RETRIES}))
  local list_artifact_url="curl -s -w \"%{http_code}\" \
  -L \
  -H 'Accept: application/vnd.github+json' \
  -H 'X-GitHub-Api-Version: 2022-11-28' \
  -H 'Authorization: Bearer $GITHUB_TOKEN' \
  \"https://api.github.com/repos/${GITHUB_REPO}/actions/artifacts?name=$sha\""

  echo "$list_artifact_url"

  while [ $retry_count -lt $max_retries ]; do
    response=$(eval "$list_artifact_url")
    http_status_code=$(echo "$response" | tail -c 4)
    response=${response%$http_status_code}
    echo "http_status_code=$http_status_code"

    artifact_url=$(echo $response | jq -r --arg sha "$sha" \
      '.artifacts[] | select(.name == $sha) | .archive_download_url')

    echo "artifact_url=$artifact_url"

    if [ -n "$artifact_url" ]; then
      curl -L -H "Authorization: Bearer $GITHUB_TOKEN" -o "${sha}.zip" "$artifact_url"
      unzip "${sha}.zip" -d "artifact"
      dotenv="artifact/${short_sha}.env"
      cat $dotenv

      cat $dotenv >> $GITHUB_ENV
      cat $dotenv >> $GITHUB_OUTPUT

      return
    else
      retry_count=$((retry_count + 1))
      echo "App Context for commit ${short_sha} not found. Retry $retry_count/$MAX_RETRIES. Sleeping for $SLEEP_TIME seconds..."
      sleep $sleep_time
    fi
  done

  echo "App Context for commit ${short_sha} after maximum retries."
  exit 1
}

mask_token

action_type=$(jq -r '.action' "$GITHUB_EVENT_PATH")
echo "action_type=$action_type"
if [ "$GITHUB_EVENT_NAME" == "pull_request" ]; then
  pr_sha=$(jq -r '.pull_request.head.sha' < "$GITHUB_EVENT_PATH")
  echo "pr_sha=${pr_sha}"
  get_app_context_from_artifact "$pr_sha"
elif [ "$GITHUB_REF" == "refs/heads/main" ]; then
  merge_sha=$(jq -r '.after' < "$GITHUB_EVENT_PATH")
  echo "merge_sha=${merge_sha}"
  get_app_context_from_artifact "$merge_sha"
else
  echo "Unsupported event context"
  exit 1
fi
