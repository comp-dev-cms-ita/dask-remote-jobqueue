#!/bin/bash
# Copyright (c) 2021 dciangot
# 
# This software is released under the MIT License.
# https://opensource.org/licenses/MIT

export _condor_AUTH_SSL_CLIENT_CAFILE=$PWD/.ca.crt
export _condor_COLLECTOR_HOST=90.147.75.109.myip.cloud.infn.it:30618
export _condor_SCHEDD_HOST=90.147.75.109.myip.cloud.infn.it
export _condor_SCHEDD_NAME=90.147.75.109.myip.cloud.infn.it
export _condor_SEC_DEFAULT_ENCRYPTION=REQUIRED
export _condor_SEC_CLIENT_AUTHENTICATION_METHODS=SCITOKENS
export _condor_SCITOKENS_FILE=$PWD/.token
export _condot_TOOL_DEBUG=D_FULLDEBUG,D_SECURITY

chmod +x job_submit.sh

# Configure oidc-agent for user token management
echo "eval \`oidc-keychain\`" >> ~/.bashrc
eval `oidc-keychain`
oidc-gen dodas --issuer $IAM_SERVER \
               --client-id $IAM_CLIENT_ID \
               --client-secret $IAM_CLIENT_SECRET \
               --rt $REFRESH_TOKEN \
               --confirm-yes \
               --scope "openid profile email" \
               --redirect-uri  http://localhost:8843 \
               --pw-cmd "echo \"DUMMY PWD\""

while true; do oidc-token dodas --time 1200 > .token; sleep 600; done &

source /usr/local/share/root6/bin/thisroot.sh
python3 start_scheduler.py
