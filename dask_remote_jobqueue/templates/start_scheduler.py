# Copyright (c) 2021 dciangot
#
# This software is released under the MIT License.
# https://opensource.org/licenses/MIT
import time
import dask
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
            python="source /usr/local/share/root6/bin/thisroot.sh ; /usr/bin/python3",
        )
        self.submit_command = "./job_submit.sh"
        self.executable = "/bin/bash"


cluster = HTCondorCluster(
    job_cls=MyHTCondorJob,
    cores=1,
    memory="3 GB",
    disk="1 GB",
    scheduler_options={
        "host": ":8989",
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
        "jhub.90.147.75.109.myip.cloud.infn.it", port=31022, username="dciangot-asdasd.dask-ssh", password="7870c6ee40f0441f873387845da4a4e1", known_hosts=None
    )
    forwarder = await connection.forward_remote_port("127.0.0.1", 8989, "127.0.0.1", 8989)
    await forwarder.wait_closed()

async def tunnel_dashboard():
    connection = await asyncssh.connect(
        "jhub.90.147.75.109.myip.cloud.infn.it", port=31022, username="dciangot-asdasd.dash.dask-ssh", password="7870c6ee40f0441f873387845da4a4e1", known_hosts=None
    )
    forwarder = await connection.forward_remote_port("127.0.0.1", 8787, "127.0.01", 8787)
    await forwarder.wait_closed()

async def tunnel():
    f1 = loop.create_task(tunnel_scheduler())
    f2 = loop.create_task(tunnel_dashboard())
    await asyncio.wait([f1, f2])

try:
    loop = asyncio.get_event_loop()
    loop.run_until_complete(tunnel())
except (OSError, asyncssh.Error) as exc:
    sys.exit('SSH connection failed: ' + str(exc))
