# Copyright (c) 2021 dciangot
#
# This software is released under the MIT License.
# https://opensource.org/licenses/MIT
import asyncio
import logging
import os
import sys
import time
from signal import SIGTERM, signal

import asyncssh  # import now to avoid adding to module startup time
import dask
import tornado.ioloop
import tornado.web
from dask_jobqueue import HTCondorCluster
from dask_jobqueue.htcondor import HTCondorJob

# dask.config.set({"distributed.worker.memory.spill": False})
# dask.config.set({"distributed.worker.memory.target": False})
logger = logging.getLogger(__name__)

loop = asyncio.get_event_loop()


class MyHTCondorJob(HTCondorJob):
    def __init__(self, *args, **kwargs):
        super().__init__(
            *args,
            **kwargs,
            python="source /cvmfs/cms.dodas.infn.it/miniconda3/etc/profile.d/conda.sh; conda activate cms-dodas; source /cvmfs/cms.dodas.infn.it/miniconda3/envs/cms-dodas/bin/thisroot.sh; export LD_LIBRARY_PATH=/cvmfs/cms.dodas.infn.it/miniconda3/envs/cms-dodas/lib:$LD_LIBRARY_PATH; python3",
        )
        self.submit_command = "./job_submit.sh"
        self.executable = "/bin/bash"


# JHUB_TOKEN={{ token }},JHUB_USER={{ name }},SCHED_PORT={{ sched_port }},DASH_PORT={{ dash_port }},
# RT={{ refresh_token }},IAM_SERVER={{ iam_server }},IAM_CLIENT_ID={{ client_id }},IAM_CLIENT_SECRET={{ client_secret }}
# _condor_AUTH_SSL_CLIENT_CAFILE={{ htc_ca }};_condor_TOOL_DEBUG={{ htc_debug }};_condor_COLLECTOR_HOST={{ htc_collector }}; _condor_SCHEDD_HOST={{ htc_schedd_host }};_condor_SCHEDD_NAME={{ htc_schedd_name }};_condor_SCITOKENS_FILE={{ htc_scitoken_file }};_condor_SEC_DEFAULT_AUTHENTICATION_METHODS={{ htc_sec_method}}

htc_ca = "$PWD/ca.crt"
# os.environ.get("_condor_AUTH_SSL_CLIENT_CAFILE")
htc_debug = os.environ.get("_condor_TOOL_DEBUG")
htc_collector = os.environ.get("_condor_COLLECTOR_HOST")
htc_schedd_host = os.environ.get("_condor_SCHEDD_HOST")
htc_schedd_name = os.environ.get("_condor_SCHEDD_NAME")
htc_scitoken_file = "$PWD/token"
# os.environ.get("_condor_SCITOKENS_FILE")
htc_sec_method = os.environ.get("_condor_SEC_DEFAULT_AUTHENTICATION_METHODS")
token = os.environ.get("JHUB_TOKEN")
name = os.environ.get("JHUB_USER")
sched_port = int(os.environ.get("SCHED_PORT"))
dash_port = int(os.environ.get("DASH_PORT"))
tornado_port = int(os.environ.get("TORNADO_PORT"))

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


async def tunnel_scheduler():
    logger.info("start tunnel scheduler")
    connection = await asyncssh.connect(
        "jhub.131.154.96.124.myip.cloud.infn.it",
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
    logger.info("start tunnel dashboard")
    connection = await asyncssh.connect(
        "jhub.131.154.96.124.myip.cloud.infn.it",
        port=31022,
        username=name,
        password=token,
        known_hosts=None,
    )
    forwarder = await connection.forward_remote_port(
        "127.0.0.1", dash_port, "127.0.0.1", dash_port
    )
    await forwarder.wait_closed()


async def tunnel_tornado():
    logger.info("start tunnel tornado")
    connection = await asyncssh.connect(
        "jhub.131.154.96.124.myip.cloud.infn.it",
        port=31022,
        username=name,
        password=token,
        known_hosts=None,
    )
    forwarder = await connection.forward_remote_port(
        "127.0.0.1", tornado_port, "127.0.0.1", tornado_port
    )
    await forwarder.wait_closed()


async def services():
    logger.info("start tunnels")
    s1 = loop.create_task(tunnel_scheduler())
    s2 = loop.create_task(tunnel_dashboard())
    s3 = loop.create_task(tunnel_tornado())
    logger.info("start tornado web")
    app = make_app()
    app.listen(tornado_port)
    tornado.ioloop.IOLoop.current().start()
    await asyncio.wait([s1, s2, s3])


class MainHandler(tornado.web.RequestHandler):
    def get(self):
        self.write("Hello, world")


def make_app():
    return tornado.web.Application(
        [
            (r"/", MainHandler),
        ]
    )


if __name__ == "__main__":
    logger.info("start main loop")
    try:
        loop.run_until_complete(services())
    except (OSError, asyncssh.Error) as exc:
        logger.error(exc)
        sys.exit("SSH connection failed: " + str(exc))
