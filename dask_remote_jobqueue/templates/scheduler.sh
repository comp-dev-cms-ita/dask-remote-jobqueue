#!/bin/bash
# Copyright (c) 2021 dciangot
# 
# This software is released under the MIT License.
# https://opensource.org/licenses/MIT

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

source /cvmfs/cms.dodas.infn.it/miniconda3/etc/profile.d/conda.sh
conda activate cms-dodas

source /cvmfs/cms.dodas.infn.it/miniconda3/envs/cms-dodas/bin/thisroot.sh

python3 start_scheduler.py
