# Copyright (c) 2021 dciangot
#
# This software is released under the MIT License.
# https://opensource.org/licenses/MIT

import logging

from .dask_remote_jobqueue import *

logging.basicConfig(filename="/var/log/dask_remote_jobqueue.log", level=logging.DEBUG)
