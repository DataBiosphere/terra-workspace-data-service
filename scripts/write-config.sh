#!/bin/bash
#
# write-config.sh extracts configuration information from vault and writes it to a set of files
# in a directory. This simplifies access to the secrets from other scripts and applications.
#
# This is intended as a replacement for render_config.sh in the service project and the
# render-config.sh in the integration project. We want to avoid writing into the source
# tree. Instead, this will write into a subdirectory of the root directory.
#

function usage {
  cat <<EOF
Usage: $0 [<target>] [<vaulttoken>] [<outputdir>] [<vaultenv>]"

  <target> can be:
    local - for testing against a local (bootRun) WDS
    dev - uses secrets from the dev environment
    alpha - alpha test environment
    staging - release staging environment
    help or ? - print this help
    clean - removes all files from the output directory
    * - anything else is assumed to be a personal environment using the terra-kernel-k8s
  If <target> is not specified, then use the envvar WDS_WRITE_CONFIG
  If WDS_WRITE_CONFIG is not specified, then use local

  <vaulttoken> defaults to the token found in ~/.vault-token.

  <outputdir> defaults to "../config/" relative to the script. When run from the gradle rootdir, it will be
  in the expected place for automation.

  <vaultenv> can be:
    docker - run a docker image containing vault
    local  - run the vault installed locally
  If <vaultenv> is not specified, then use the envvar WDS_VAULT_ENV
  If WDS_VAULT_ENV is not specified, then we use docker
EOF
 exit 1
}

# Get the inputs with defaulting
script_dir="$( cd "$( dirname "${BASH_SOURCE[0]}" )/.." &> /dev/null  && pwd )"
default_outputdir="${script_dir}/config"
default_target=${WDS_WRITE_CONFIG:-local}
target=${1:-$default_target}
vaulttoken=${2:-$(cat "$HOME"/.vault-token)}
outputdir=${3:-$default_outputdir}
default_vaultenv=${WDS_VAULT_ENV:-docker}
vaultenv=${4:-$default_vaultenv}


# Create the output directory if it doesn't already exist
mkdir -p "${outputdir}"

# If there is a config and it matches, don't regenerate
if [ -e "${outputdir}/target.txt" ]; then
    oldtarget=$(<"${outputdir}/target.txt")
    if [ "$oldtarget" = "$target" ]; then
        echo "Config for $target already written"
        exit 0
    fi
fi

# Run vault either using a docker container or using the installed vault
function dovault {
    local dovaultpath=$1
    local dofilename=$2
    case $vaultenv in
        docker)
            docker run --rm -e VAULT_TOKEN="${vaulttoken}" broadinstitute/dsde-toolbox:consul-0.20.0 \
                   vault read -format=json "${dovaultpath}" > "${dofilename}"
            ;;

        local)
            VAULT_TOKEN="${vaulttoken}" VAULT_ADDR="https://clotho.broadinstitute.org:8200" \
                   vault read -format=json "${dovaultpath}" > "${dofilename}"
            ;;
    esac
}

# Read a vault path into an output file
function vaultget {
    vaultpath=$1
    filename=$2
    fntmpfile=$(mktemp)
    dovault "${vaultpath}" "${fntmpfile}"
    result=$?
    if [ $result -ne 0 ]; then return $result; fi
    jq -r .data "${fntmpfile}" > "${filename}"
}

vaultget "secret/dsde/${target}/workspace-data-service/dummy-secret" "${outputdir}/dummy-secret.json"

# We made it to the end, so record the target and avoid redos TODO do we want to keep this mechanism
echo "$target" > "${outputdir}/target.txt"
