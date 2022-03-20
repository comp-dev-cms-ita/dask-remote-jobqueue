#!/bin/bash
# Copyright (c) 2021 dciangot
#
# This software is released under the MIT License.
# https://opensource.org/licenses/MIT

source /cvmfs/cms.dodas.infn.it/miniconda3/bin/activate
conda activate af-test

export _condor_AUTH_SSL_CLIENT_CAFILE={{ htc_ca }}
export _condor_TOOL_DEBUG={{ htc_debug }}
export _condor_COLLECTOR_HOST={{ htc_collector }}
export _condor_SCHEDD_HOST={{ htc_schedd_host }}
export _condor_SCHEDD_NAME={{ htc_schedd_name }}
export _condor_SCITOKENS_FILE={{ htc_scitoken_file }}
export _condor_SEC_DEFAULT_AUTHENTICATION_METHODS={{ htc_sec_method}}

# sleep $((($RANDOM % 60) + 1))s

condor_submit -spool $@
