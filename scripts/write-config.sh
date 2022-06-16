# Simple script for pulling local vault credentials. All arguments are optional.
# Usage is: ./scripts/write-config.sh [env] [execution env] [vaulttoken]

ENV=${1:-dev}
EXECUTION_ENV=${2:-docker}
VAULT_TOKEN=${3:-$(cat "$HOME"/.vault-token)}

VAULT_ADDR="https://clotho.broadinstitute.org:8200"
WDS_VAULT_PATH="secret/dsde/$ENV/workspacedata"

function runVaultInEnv {
    case $EXECUTION_ENV in
        local)
            VAULT_TOKEN=$VAULT_TOKEN VAULT_ADDR=$VAULT_ADDR \
                vault read -format=json "$1"
            ;;
        docker)
            docker run --rm -e VAULT_TOKEN=$VAULT_TOKEN -e VAULT_ADDR=$VAULT_ADDR vault:1.7.3 \
                vault read -format=json "$1"
            ;;
        *)
            echo "Invalid input: Execution env must be docker or local. For example: $0 <env> <token> local"
            ;;
    esac
}

function dovault {
    runVaultInEnv $1 | jq -r .data
}

OUTPUT_LOCATION="$(dirname "$0")/../config"
mkdir -p $OUTPUT_LOCATION

dovault "$WDS_VAULT_PATH/dummy-secret" > "$OUTPUT_LOCATION/dummy-secret.json"
