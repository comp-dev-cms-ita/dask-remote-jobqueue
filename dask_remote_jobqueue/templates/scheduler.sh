#!/usr/bin/env bash
# Copyright (c) 2021 dciangot
#
# This software is released under the MIT License.
# https://opensource.org/licenses/MIT


chmod +x job_submit.sh
chmod +x job_rm.sh
chmod +x entrypoint.sh

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
    curl -d grant_type=urn:ietf:params:oauth:grant-type:token-exchange \
        -u $IAM_CLIENT_ID:$IAM_CLIENT_SECRET \
        -d audience="https://wlcg.cern.ch/jwt/v1/any" \
        -d subject_token=`cat token` \
        -d scope="openid profile wlcg wlcg.groups" \
        ${IAM_SERVER}/token \
        | tee /tmp/response | jq .access_token |  tr -d '"' |  tr -d '\n'> /tmp/token_tmp \
    && cp /tmp/token_tmp token
    sleep 72000
done &

source /cvmfs/cms.dodas.infn.it/miniconda3/bin/activate
conda activate af-test

if command -V tini &>/dev/null; then
    tini -s python3 -- start_scheduler.py
else
    python3 start_scheduler.py
fi
