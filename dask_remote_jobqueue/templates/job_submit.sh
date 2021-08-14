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

condor_submit -spool $@
