#!/usr/bin/env bash
# Copyright (c) 2021 dciangot
#
# This software is released under the MIT License.
# https://opensource.org/licenses/MIT

mkdir -p "$(pwd)/.oidc-agent"

OIDC_CONFIG_DIR="$(pwd)/.oidc-agent"
export OIDC_CONFIG_DIR

chmod +x job_submit.sh
chmod +x job_rm.sh

# Configure oidc-agent for user token management
# Ref: https://indigo-dc.gitbook.io/oidc-agent/user/oidc-keychain
echo -e "\nOIDC_CONFIG_DIR=\"$(pwd)/.oidc-agent\"" >>.bashrc
echo -e "\nexport OIDC_CONFIG_DIR" >>.bashrc
echo -e "\neval \`oidc-keychain\`" >>.bashrc

eval $(oidc-keychain)

oidc-gen dodas --issuer "$IAM_SERVER" \
    --client-id "$IAM_CLIENT_ID" \
    --client-secret "$IAM_CLIENT_SECRET" \
    --rt "$REFRESH_TOKEN" \
    --confirm-yes \
    --scope "openid profile email" \
    --redirect-uri http://localhost:8843 \
    --pw-cmd "echo \"DUMMY PWD\""

while true; do
    oidc-token dodas --time 1200 >token
    sleep 600
done &

source /cvmfs/cms.dodas.infn.it/miniconda3/etc/profile.d/conda.sh
conda activate cms-dodas

if command -V tini &>/dev/null; then
    tini -s python3 -- start_scheduler.py
else
    python3 start_scheduler.py
fi
