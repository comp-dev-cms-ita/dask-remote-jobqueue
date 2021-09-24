# Copyright (c) 2021 dciangot
#
# This software is released under the MIT License.
# https://opensource.org/licenses/MIT
import time
import dask
import os
from dask_jobqueue import HTCondorCluster
from dask_jobqueue.htcondor import HTCondorJob
from signal import SIGTERM, signal

# dask.config.set({"distributed.worker.memory.spill": False})
# dask.config.set({"distributed.worker.memory.target": False})


class MyHTCondorJob(HTCondorJob):
    def __init__(self, *args, **kwargs):
        super().__init__(
            *args,
            **kwargs,
            python="source /cvmfs/cms.dodas.infn.it/miniconda3/envs/cms-dodas/bin/thisroot.sh; /usr/bin/python3",
        )
        self.submit_command = "./job_submit.sh"
        self.executable = "/bin/bash"


# JHUB_TOKEN={{ token }},JHUB_USER={{ name }},SCHED_PORT={{ sched_port }},DASH_PORT={{ dash_port }},
# RT={{ refresh_token }},IAM_SERVER={{ iam_server }},IAM_CLIENT_ID={{ client_id }},IAM_CLIENT_SECRET={{ client_secret }}
#_condor_AUTH_SSL_CLIENT_CAFILE={{ htc_ca }};_condor_TOOL_DEBUG={{ htc_debug }};_condor_COLLECTOR_HOST={{ htc_collector }}; _condor_SCHEDD_HOST={{ htc_schedd_host }};_condor_SCHEDD_NAME={{ htc_schedd_name }};_condor_SCITOKENS_FILE={{ htc_scitoken_file }};_condor_SEC_DEFAULT_AUTHENTICATION_METHODS={{ htc_sec_method}}

htc_ca = "$PWD/ca.crt" 
#os.environ.get("_condor_AUTH_SSL_CLIENT_CAFILE")
htc_debug = os.environ.get("_condor_TOOL_DEBUG")
htc_collector = os.environ.get("_condor_COLLECTOR_HOST")
htc_schedd_host = os.environ.get("_condor_SCHEDD_HOST")
htc_schedd_name = os.environ.get("_condor_SCHEDD_NAME")
htc_scitoken_file = "$PWD/token"
#os.environ.get("_condor_SCITOKENS_FILE")
htc_sec_method = os.environ.get("_condor_SEC_DEFAULT_AUTHENTICATION_METHODS")
token = os.environ.get("JHUB_TOKEN")
name = os.environ.get("JHUB_USER")
sched_port = int(os.environ.get("SCHED_PORT"))
dash_port = int(os.environ.get("DASH_PORT"))

cluster = HTCondorCluster(
    job_cls=MyHTCondorJob,
    cores=1,
    memory="3 GB",
    disk="1 GB",
    scheduler_options={
        "host": ":{}".format(sched_port),
        "dashboard_address": "127.0.0.1:{}".format(dash_port),
    },
    job_extra={
        "+OWNER": '"condor"',
        "log": "simple.log",
        "output": "simple.out",
        "error": "simple.error",
    },
    silence_logs="debug",
)

adapt = cluster.adapt(minimum=1, maximum=15)

# cluster.scale(jobs=3)

import asyncssh  # import now to avoid adding to module startup time
import asyncio
import sys


async def tunnel_scheduler():
    connection = await asyncssh.connect(
        "jhub.90.147.75.109.myip.cloud.infn.it",
        port=31022,
        username=name,
        password=token,
        known_hosts=None,
    )
    forwarder = await connection.forward_remote_port(
        "127.0.0.1", sched_port, "127.0.0.1", sched_port
    )
    await forwarder.wait_closed()


async def tunnel_dashboard():
    connection = await asyncssh.connect(
        "jhub.90.147.75.109.myip.cloud.infn.it",
        port=31022,
        username=name,
        password=token,
        known_hosts=None,
    )
    forwarder = await connection.forward_remote_port(
        "127.0.0.1", dash_port, "127.0.0.1", dash_port
    )
    await forwarder.wait_closed()


async def tunnel():
    f1 = loop.create_task(tunnel_scheduler())
    f2 = loop.create_task(tunnel_dashboard())
    await asyncio.wait([f1, f2])


try:
    loop = asyncio.get_event_loop()
    loop.run_until_complete(tunnel())
except (OSError, asyncssh.Error) as exc:
    sys.exit("SSH connection failed: " + str(exc))
