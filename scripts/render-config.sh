# Simple script for pulling local vault credentials. All arguments are optional.
# Usage is: ./scripts/render-config.sh [wds env] [vault env] [vaulttoken]
# wds env options are: local, dev, perf, staging, prod
# vault env options are: local, docker
# vaulttoken: put your vault token here

WDS_ENV=${1:-local}
VAULT_ENV=${2:-docker}
VAULT_TOKEN=${3:-$(cat "$HOME"/.vault-token)}

VAULT_ADDR="https://clotho.broadinstitute.org:8200"
WDS_VAULT_PATH="secret/dsde/$WDS_ENV/workspacedata"

case $VAULT_ENV in
    local | docker) ;;
    *)
        echo "Invalid input: Vault env must be docker or local.\n" \
            "For example: /scripts/render-config.sh dev local"
        exit 0
        ;;
esac

function runVaultInEnv {
    case $VAULT_ENV in
        local)
            VAULT_TOKEN=$VAULT_TOKEN VAULT_ADDR=$VAULT_ADDR \
                vault read -format=json "$1"
            ;;
        docker)
            docker run --rm -e VAULT_TOKEN=$VAULT_TOKEN -e VAULT_ADDR=$VAULT_ADDR vault:1.7.3 \
                vault read -format=json "$1"
            ;;
    esac
}

function dovault {
    runVaultInEnv $1 | jq -r .data
}

OUTPUT_LOCATION="$(dirname "$0")/../config"
mkdir -p $OUTPUT_LOCATION

dovault "$WDS_VAULT_PATH/dummy-secret" > "$OUTPUT_LOCATION/dummy-secret.json"
