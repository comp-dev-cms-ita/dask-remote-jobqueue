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
    await connection.forward_remote_port("localhost", 8989, "localhost", 8989)

async def tunnel_dashboard():
    connection = await asyncssh.connect(
        "jhub.90.147.75.109.myip.cloud.infn.it", port=31022, username="dciangot-asdasd.dash.dask-ssh", password="7870c6ee40f0441f873387845da4a4e1", known_hosts=None
    )
    await connection.forward_remote_port("localhost", 8787, "localhost", 8787)

try:
    asyncio.get_event_loop().run_until_complete(tunnel_scheduler())
except (OSError, asyncssh.Error) as exc:
    sys.exit('SSH connection failed: ' + str(exc))

try:
    asyncio.get_event_loop().run_until_complete(tunnel_dashboard())
except (OSError, asyncssh.Error) as exc:
    sys.exit('SSH connection failed: ' + str(exc))


while True:
    time.sleep(60)
